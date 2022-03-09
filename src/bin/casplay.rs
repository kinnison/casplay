use std::{
    collections::HashMap,
    fs::metadata,
    io::{stdout, Write},
    path::{Path, PathBuf},
};

use std::os::unix::fs::PermissionsExt;

use async_recursion::async_recursion;
use async_stream::stream;
use casplay::{
    build::bazel::remote::execution::v2::{
        capabilities_client::CapabilitiesClient,
        content_addressable_storage_client::ContentAddressableStorageClient, Digest, Directory,
        DirectoryNode, FileNode, FindMissingBlobsRequest, GetCapabilitiesRequest, GetTreeRequest,
        SymlinkNode,
    },
    google::bytestream::{byte_stream_client::ByteStreamClient, ReadRequest, WriteRequest},
    Uploader,
};

use clap::{Parser, Subcommand};
use prost::Message;
use sha256::{digest_bytes, digest_file};
use tokio::fs::{read_dir, read_link, File};
use tokio::io::AsyncReadExt;
use tracing::info;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about=None)]
struct Cli {
    #[clap(long)]
    endpoint: Option<String>,
    #[clap(long)]
    instance: Option<String>,

    #[clap(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    Caps,
    Fetch { name: String },
    Upload { name: PathBuf },
    Ls { name: String },
    Serve,
}

type Crapshoot = anyhow::Result<()>;

#[tokio::main]
async fn main() -> Crapshoot {
    real_main().await
}

struct Config {
    endpoint: String,
    instance_name: String,
}

async fn real_main() -> Crapshoot {
    let cli = Cli::parse();

    let endpoint = cli
        .endpoint
        .unwrap_or_else(|| "http://localhost:50052".into());

    let config = Config {
        endpoint,
        instance_name: cli.instance.unwrap_or_else(|| "".into()),
    };

    match cli.command {
        Command::Caps => capabilities(&config).await?,
        Command::Fetch { name } => fetch_blob(&config, &name).await?,
        Command::Upload { name } => upload_blob(&config, &name).await?,
        Command::Ls { name } => list_tree(&config, &name).await?,
        Command::Serve => serve(&config).await?,
    };

    Ok(())
}

async fn serve(config: &Config) -> Crapshoot {
    tracing_subscriber::fmt().init();
    info!("Starting server on port 5000");
    casplay::server::serve(([0, 0, 0, 0], 5000).into(), &config.instance_name).await?;
    Ok(())
}

async fn capabilities(config: &Config) -> Crapshoot {
    let request = GetCapabilitiesRequest {
        instance_name: config.instance_name.clone(),
    };

    let mut client = CapabilitiesClient::connect(config.endpoint.clone()).await?;

    let caps = client.get_capabilities(request).await?;

    let cachecaps = caps.into_inner().cache_capabilities.unwrap();

    println!("Output: {:#?}", cachecaps);

    Ok(())
}

async fn fetch_blob(config: &Config, name: &str) -> Crapshoot {
    let mut client = ByteStreamClient::connect(config.endpoint.clone()).await?;
    let request = ReadRequest {
        resource_name: format!("{}/blobs/{}", config.instance_name, name),
        read_offset: 0,
        read_limit: 0,
    };

    let mut reader = client.read(request).await?.into_inner();

    while let Some(msg) = reader.message().await? {
        stdout().write_all(&msg.data).unwrap();
    }

    Ok(())
}

async fn upload_blob(config: &Config, name: &Path) -> Crapshoot {
    if metadata(name)?.is_dir() {
        return upload_dir(config, name).await;
    }

    let digest = really_upload_blob(config, name).await?;

    println!("Blob uploaded as {}/{}", digest.hash, digest.size_bytes);
    Ok(())
}

async fn really_upload_blob(config: &Config, name: &Path) -> anyhow::Result<Digest> {
    let sha = digest_file(name)?;
    let flen = metadata(name)?.len();

    // if has_blob(config, &sha, flen).await? {
    //     return Ok(Digest {
    //         hash: sha,
    //         size_bytes: flen as i64,
    //     });
    // }

    let base_request = WriteRequest {
        resource_name: format!(
            "{}/uploads/idontknowwhatgoeshere/blobs/{}/{}",
            config.instance_name, sha, flen
        ),
        write_offset: 0,
        finish_write: false,
        data: Vec::new(),
    };

    let mut client = ByteStreamClient::connect(config.endpoint.clone()).await?;

    let mut fh = File::open(name).await?;
    let mut buffer = Vec::with_capacity(4096);

    let stream = stream! {
        let mut total_bytes = 0;
        if loop {
            buffer.resize(4096,0u8);
            let bytecount = match fh.read(&mut buffer[..]).await {
                Ok(n) => n,
                Err(_) => break false,
            };
            buffer.resize(bytecount,0);
            let mut request = base_request.clone();
            request.write_offset = total_bytes;
            request.data = std::mem::replace(&mut buffer, Vec::with_capacity(4096));
            yield request;
            total_bytes += bytecount as i64;
            if bytecount != 4096 {
                break true;
            } }{
                let mut request = base_request.clone();
                request.write_offset = total_bytes;
                request.finish_write = true;
                yield request;
            }

    };

    println!("About to call write!");
    let response = client.write(stream).await?;

    if response.get_ref().committed_size != flen as i64 {
        anyhow::bail!("Woah, committed size was wrong");
    }

    Ok(Digest {
        hash: sha,
        size_bytes: flen as i64,
    })
}

#[allow(dead_code)]
async fn has_blob(config: &Config, sha: &str, flen: u64) -> anyhow::Result<bool> {
    let mut client = ContentAddressableStorageClient::connect(config.endpoint.clone()).await?;
    let request = FindMissingBlobsRequest {
        instance_name: config.instance_name.clone(),
        blob_digests: vec![Digest {
            hash: sha.to_string(),
            size_bytes: flen as i64,
        }],
    };
    let response = client.find_missing_blobs(request).await?.into_inner();
    Ok(response.missing_blob_digests.is_empty())
}

fn digest_from_str(input: &str) -> Digest {
    let parts: Vec<_> = input.split('/').collect();
    assert_eq!(parts.len(), 2);
    Digest {
        hash: parts[0].into(),
        size_bytes: parts[1].parse().unwrap(),
    }
}

async fn list_tree(config: &Config, name: &str) -> Crapshoot {
    let mut client = ContentAddressableStorageClient::connect(config.endpoint.clone()).await?;
    let root_digest = digest_from_str(name);
    let request = GetTreeRequest {
        instance_name: config.instance_name.clone(),
        root_digest: Some(root_digest.clone()),
        page_size: 100,
        page_token: Default::default(),
    };

    let mut response = client.get_tree(request).await?.into_inner();

    let mut namemap: HashMap<String, String> = HashMap::new();

    namemap.insert(
        format!("{}/{}", root_digest.hash, root_digest.size_bytes),
        "".into(),
    );

    while let Some(response) = response.message().await? {
        for dir in response.directories.iter() {
            let enc = dir.encode_to_vec();
            let sha = digest_bytes(&enc);
            let digest = format!("{}/{}", sha, enc.len());

            let this_name = namemap.get(&digest).unwrap().to_string();
            for file in &dir.files {
                if let Some(digest) = &file.digest {
                    println!(
                        "{}/{} is {} file {}/{}",
                        this_name,
                        file.name,
                        if file.is_executable {
                            "an executable"
                        } else {
                            "a"
                        },
                        digest.hash,
                        digest.size_bytes
                    );
                } else {
                    println!(
                        "{}/{} is {} file (No digest?)",
                        this_name,
                        file.name,
                        if file.is_executable {
                            "an executable"
                        } else {
                            "a"
                        }
                    );
                }
            }
            for dir in &dir.directories {
                if let Some(digest) = &dir.digest {
                    namemap.insert(
                        format!("{}/{}", digest.hash, digest.size_bytes),
                        format!("{}/{}", this_name, dir.name),
                    );
                }
            }
            for link in &dir.symlinks {
                println!("{}/{} -> {}", this_name, link.name, link.target);
            }
        }
    }

    Ok(())
}

async fn upload_dir(config: &Config, name: &Path) -> Crapshoot {
    let mut uploader = Uploader::new(
        config.endpoint.clone(),
        Some(config.instance_name.as_ref()),
        None,
    )
    .await?;
    let res = upload_dir_(&mut uploader, config, name, "").await?;
    uploader.flush().await?;
    println!(
        "Uploaded directory {}, digest is {}/{}",
        name.display(),
        res.hash,
        res.size_bytes
    );
    Ok(())
}

#[async_recursion]
async fn upload_dir_(
    uploader: &mut Uploader,
    config: &Config,
    dir: &Path,
    base_name: &str,
) -> anyhow::Result<Digest> {
    let mut dir_reader = read_dir(dir).await?;

    let mut this_dir = Directory {
        files: vec![],
        directories: vec![],
        symlinks: vec![],
        node_properties: None,
    };

    while let Some(entry) = dir_reader.next_entry().await? {
        // Do something with the entry
        let ftype = entry.file_type().await?;
        let name = entry.file_name().to_string_lossy().to_string();
        if ftype.is_dir() {
            let subname = format!("{}{}/", base_name, name);
            let digest =
                upload_dir_(uploader, config, &dir.join(entry.file_name()), &subname).await?;
            let node = DirectoryNode {
                name: name.clone(),
                digest: Some(digest),
            };
            this_dir.directories.push(node);
        }
        if ftype.is_file() {
            let digest = uploader.upload_file(&entry.path()).await?;
            let perms = entry.metadata().await?.permissions();
            let executable = (perms.mode() & 0o111) != 0;
            let node = FileNode {
                name: name.clone(),
                digest: Some(digest),
                is_executable: executable,
                node_properties: None,
            };
            this_dir.files.push(node);
        }
        if ftype.is_symlink() {
            let target = read_link(entry.path()).await?;
            let target = format!("{}", target.display());
            let node = SymlinkNode {
                name: name.clone(),
                target,
                node_properties: None,
            };
            this_dir.symlinks.push(node);
        }
    }

    this_dir.files.sort_by_key(|e| e.name.clone());
    this_dir.directories.sort_by_key(|e| e.name.clone());
    this_dir.symlinks.sort_by_key(|e| e.name.clone());

    Ok(uploader.queue_message(this_dir).await?)
}
