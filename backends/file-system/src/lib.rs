//
// Copyright (c) 2017, 2020 ADLINK Technology Inc.
//
// This program and the accompanying materials are made available under the
// terms of the Eclipse Public License 2.0 which is available at
// http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
// which is available at https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
//
// Contributors:
//   ADLINK zenoh team, <zenoh@adlink-labs.tech>
//
#![feature(async_closure)]

use async_trait::async_trait;
use log::{debug, error, warn};
use std::convert::TryFrom;
use std::fs::{metadata, remove_file, DirBuilder, File, Metadata};
use std::io::prelude::*;
use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tempfile::tempfile_in;
use walkdir::WalkDir;
use zenoh::net::utils::resource_name;
use zenoh::net::{encoding, DataInfo, RBuf, Sample};
use zenoh::{
    Change, ChangeKind, Properties, Selector, Timestamp, TimestampID, Value, ZError, ZErrorKind,
    ZResult,
};
use zenoh_backend_traits::*;
use zenoh_util::collections::{Timed, TimedEvent, TimedHandle, Timer};
use zenoh_util::{zerror, zerror2};

// Properies used by the Backend

// Properies used by the Storage
pub const PROP_STORAGE_READ_ONLY: &str = "read_only";
pub const PROP_STORAGE_DIR: &str = "dir";
pub const PROP_STORAGE_ON_CLOSURE: &str = "on_closure";
pub const PROP_STORAGE_FOLLOW_LINK: &str = "follow_links";

// delay after deletion to drop a measurement
const DROP_MEASUREMENT_TIMEOUT_MS: u64 = 5000;

#[no_mangle]
pub fn create_backend(properties: &Properties) -> ZResult<Box<dyn Backend>> {
    // For some reasons env_logger is sometime not active in a loaded library.
    // Try to activate it here, ignoring failures.
    let _ = env_logger::try_init();

    let admin_status = zenoh::utils::properties_to_json_value(properties);

    Ok(Box::new(FileSystemBackend { admin_status }))
}

pub struct FileSystemBackend {
    admin_status: Value,
}

#[async_trait]
impl Backend for FileSystemBackend {
    async fn get_admin_status(&self) -> Value {
        self.admin_status.clone()
    }

    async fn create_storage(&mut self, props: Properties) -> ZResult<Box<dyn Storage>> {
        let path_expr = props.get(PROP_STORAGE_PATH_EXPR).unwrap();
        let path_prefix = props
            .get(PROP_STORAGE_PATH_PREFIX)
            .ok_or_else(|| {
                zerror2!(ZErrorKind::Other {
                    descr: format!(
                        r#"Missing required property for File System Storage: "{}""#,
                        PROP_STORAGE_PATH_PREFIX
                    )
                })
            })?
            .clone();
        if !path_expr.starts_with(&path_prefix) {
            return zerror!(ZErrorKind::Other {
                descr: format!(
                    r#"The specified "{}={}" is not a prefix of "{}={}""#,
                    PROP_STORAGE_PATH_PREFIX, path_prefix, PROP_STORAGE_PATH_EXPR, path_expr
                )
            });
        }

        let read_only = props.contains_key(PROP_STORAGE_READ_ONLY);
        let on_closure = OnClosure::try_from(&props)?;
        let follow_links = match props.get(PROP_STORAGE_FOLLOW_LINK) {
            Some(s) => {
                if s.eq_ignore_ascii_case("false") || s.eq_ignore_ascii_case("no") {
                    false
                } else {
                    return zerror!(ZErrorKind::Other {
                        descr: format!(
                            r#"Invalid value for File System Storage property "{}={}""#,
                            PROP_STORAGE_FOLLOW_LINK, s
                        )
                    });
                }
            }
            None => true,
        };

        let base_dir = props
            .get(PROP_STORAGE_DIR)
            .ok_or_else(|| {
                zerror2!(ZErrorKind::Other {
                    descr: format!(
                        r#"Missing required property for File System Storage: "{}""#,
                        PROP_STORAGE_DIR
                    )
                })
            })?
            .clone();

        // check if base_dir exists and is readable (and writeable if not "read_only" mode)
        let mut dir_builder = DirBuilder::new();
        dir_builder.recursive(true);
        let base_dir_path = PathBuf::from(&base_dir);
        if !base_dir_path.exists() {
            if let Err(err) = dir_builder.create(&base_dir) {
                return zerror!(ZErrorKind::Other {
                    descr: format!(
                        r#"Cannot create File System Storage on "dir"={} : {}"#,
                        base_dir, err
                    )
                });
            }
        } else if !base_dir_path.is_dir() {
            return zerror!(ZErrorKind::Other {
                descr: format!(
                    r#"Cannot create File System Storage on "dir"={} : this is not a directory"#,
                    base_dir
                )
            });
        } else if let Err(err) = base_dir_path.read_dir() {
            return zerror!(ZErrorKind::Other {
                descr: format!(
                    r#"Cannot create File System Storage on "dir"={} : {}"#,
                    base_dir, err
                )
            });
        } else if !read_only {
            // try to write a random file
            let _ = tempfile_in(&base_dir)
                .map(|mut f| writeln!(f, "test"))
                .map_err(|err| {
                    zerror2!(ZErrorKind::Other {
                        descr: format!(
                            r#"Cannot create writeable File System Storage on "dir"={} : {}"#,
                            base_dir, err
                        )
                    })
                })?;
        }

        let admin_status = zenoh::utils::properties_to_json_value(&props);
        Ok(Box::new(FileSystemStorage {
            admin_status,
            base_dir,
            path_prefix,
            dir_builder,
            follow_links,
            read_only,
            on_closure,
            timer: Timer::new(),
        }))
    }

    fn incoming_data_interceptor(&self) -> Option<Box<dyn IncomingDataInterceptor>> {
        None
    }

    fn outgoing_data_interceptor(&self) -> Option<Box<dyn OutgoingDataInterceptor>> {
        None
    }
}

enum OnClosure {
    DeleteAll,
    DoNothing,
}

impl TryFrom<&Properties> for OnClosure {
    type Error = ZError;
    fn try_from(p: &Properties) -> ZResult<OnClosure> {
        match p.get(PROP_STORAGE_ON_CLOSURE) {
            Some(s) => {
                if s == "delete_all" {
                    Ok(OnClosure::DeleteAll)
                } else {
                    zerror!(ZErrorKind::Other {
                        descr: format!("Unsupported value for 'on_closure' property: {}", s)
                    })
                }
            }
            None => Ok(OnClosure::DoNothing),
        }
    }
}

struct FileSystemStorage {
    admin_status: Value,
    base_dir: String,
    path_prefix: String,
    dir_builder: DirBuilder,
    follow_links: bool,
    read_only: bool,
    on_closure: OnClosure,
    timer: Timer,
}

impl FileSystemStorage {
    async fn schedule_measurement_drop(&self, file: PathBuf) -> TimedHandle {
        let event = TimedEvent::once(
            Instant::now() + Duration::from_millis(DROP_MEASUREMENT_TIMEOUT_MS),
            TimedFileCleanup { file },
        );
        let handle = event.get_handle();
        self.timer.add(event).await;
        handle
    }

    async fn reply_with_matching_files(&self, query: &Query, path_expr: &str) {
        // find the longest segment without '*' to search for files only in the corresponding
        let star_idx = path_expr.find('*').unwrap();
        let segment = match path_expr[..star_idx].rfind('/') {
            Some(i) => &path_expr[..i],
            None => "",
        };
        let dir = concat_str(&self.base_dir, segment);
        let base_dir_len = self.base_dir.len();

        debug!(
            "For query on {} search matching files in {}",
            query.res_name(),
            dir
        );
        let walkdir = WalkDir::new(&dir).follow_links(self.follow_links);
        // TODO: optimization calling self.reply_with_file() in a task and then join all
        for entry in walkdir.into_iter() {
            match entry {
                Ok(e) => {
                    if e.file_type().is_file() {
                        if let Some(p) = e.path().to_str() {
                            let shortpath = &p[base_dir_len..];
                            if resource_name::intersect(shortpath, path_expr) {
                                self.reply_with_file(query, shortpath).await;
                            }
                        }
                    }
                }
                Err(e) => {
                    warn!("Error exploring directory {}: {}", dir, e);
                }
            }
        }
    }

    async fn reply_with_file(&self, query: &Query, shortpath: &str) {
        // zenoh path for this file is: self.path_prefix + shortpath
        let zpath = concat_str(&self.path_prefix, shortpath);

        // real path for this file is: self.base_dir + shortpath
        let file = concat_str(&self.base_dir, shortpath);

        match File::open(&file) {
            Ok(mut f) => {
                // TODO: what if file is too big ??
                let size = f.metadata().map(|m| m.len()).unwrap_or(256);
                if size <= usize::MAX as u64 {
                    let mut content: Vec<u8> = Vec::with_capacity(size as usize);
                    if let Err(e) = f.read_to_end(&mut content) {
                        warn!(
                            "Replying to query on {} : failed to read file {} : {}",
                            query.res_name(),
                            file,
                            e
                        );
                        return;
                    }
                    match self.get_data_info(&file) {
                        Ok(data_info) => {
                            debug!("Reply file {} for path {}", file, zpath);
                            query
                                .reply(Sample {
                                    res_name: zpath,
                                    payload: RBuf::from(content),
                                    data_info: Some(data_info),
                                })
                                .await;
                        }
                        Err(e) => warn!("{}", e),
                    }
                } else {
                    warn!(
                        "Replying to query on {} : file {} is too big to fit in memory",
                        query.res_name(),
                        file
                    )
                }
            }
            Err(e) => warn!(
                "Replying to query on {} : failed to open file {} : {}",
                query.res_name(),
                file,
                e
            ),
        }
    }

    fn get_data_info(&self, file: &str) -> ZResult<DataInfo> {
        // TODO: try to get DataInfo from meta-data file

        // guess mime type from file extension
        let mime_type = mime_guess::from_path(file).first_or_octet_stream();
        debug!("Found mime-type {} for {}", mime_type, file);
        let encoding =
            encoding::from_str(mime_type.essence_str()).unwrap_or(encoding::APP_OCTET_STREAM);

        // create timestamp from file's modification time
        let metadata = metadata(file).map_err(|e| {
            zerror2!(ZErrorKind::Other {
                descr: format!("Failed to get meta-data for file '{}': {}", file, e)
            })
        })?;
        let sys_time = metadata
            .modified()
            .or_else(|_| metadata.accessed())
            .or_else(|_| metadata.created())
            .unwrap_or_else(|_| SystemTime::now());
        let timestamp = Timestamp::new(
            sys_time.duration_since(UNIX_EPOCH).unwrap().into(),
            TimestampID::new(1, [0u8; TimestampID::MAX_SIZE]),
        );

        Ok(DataInfo {
            source_id: None,
            source_sn: None,
            first_router_id: None,
            first_router_sn: None,
            timestamp: Some(timestamp),
            kind: None,
            encoding: Some(encoding),
        })
    }

    fn get_timestamp(&self, file: &str) -> ZResult<Timestamp> {
        // TODO: try to get Timestamp from meta-data file

        // create timestamp from file's modification time
        let metadata = metadata(file).map_err(|e| {
            zerror2!(ZErrorKind::Other {
                descr: format!("Failed to get meta-data for file '{}': {}", file, e)
            })
        })?;
        let sys_time = metadata
            .modified()
            .or_else(|_| metadata.accessed())
            .or_else(|_| metadata.created())
            .unwrap_or_else(|_| SystemTime::now());
        Ok(Timestamp::new(
            sys_time.duration_since(UNIX_EPOCH).unwrap().into(),
            TimestampID::new(1, [0u8; TimestampID::MAX_SIZE]),
        ))
    }
}

#[async_trait]
impl Storage for FileSystemStorage {
    async fn get_admin_status(&self) -> Value {
        self.admin_status.clone()
    }

    // When receiving a Sample (i.e. on PUT or DELETE operations)
    async fn on_sample(&mut self, sample: Sample) -> ZResult<()> {
        let change = Change::from_sample(sample, false)?;

        // file path (relative to base_dir) is the zenoh path stripped of "path_prefix"
        let file_path = change
            .path
            .as_str()
            .strip_prefix(&self.path_prefix)
            .ok_or_else(|| {
                zerror2!(ZErrorKind::Other {
                    descr: format!(
                        "Received a Sample not starting with path_prefix '{}'",
                        self.path_prefix
                    )
                })
            })?;

        // Store or delete the sample depending the ChangeKind
        match change.kind {
            ChangeKind::PUT => {
                if !self.read_only {
                    // TODO
                    error!("UNIMPL");

                    // get timestamp of deletion of this measurement, if any

                    // encode the value as a string to be stored in InfluxDB

                    // Note: tags are stored as strings in InfluxDB, while fileds are typed.
                    // For simpler/faster deserialization, we store encoding, timestamp and base64 as fields.
                    // while the kind is stored as a tag to be indexed by InfluxDB and have faster queries on it.
                } else {
                    warn!(
                        "Received PUT for read-only Files System Storage on {} - ignored",
                        self.base_dir
                    );
                }
            }
            ChangeKind::DELETE => {
                if !self.read_only {
                    // TODO
                    error!("UNIMPL");

                    // delete all points from the measurement that are older than this DELETE message
                    // (in case more recent PUT have been recevived un-ordered)

                    // store a point (with timestamp) with "delete" tag, thus we don't re-introduce an older point later

                    // schedule the drop of measurement later in the future, if it's empty
                } else {
                    warn!(
                        "Received DELETE for read-only Files System Storage on {} - ignored",
                        self.base_dir
                    );
                }
            }
            ChangeKind::PATCH => {
                println!("Received PATCH for {}: not yet supported", change.path);
            }
        }
        Ok(())
    }

    // When receiving a Query (i.e. on GET operations)
    async fn on_query(&mut self, query: Query) -> ZResult<()> {
        // get the query's Selector
        let selector = Selector::try_from(&query)?;

        // get the list of sub-path expressions that will match the same stored keys than
        // the selector, if those keys had the path_prefix.
        let path_exprs = utils::get_sub_path_exprs(selector.path_expr.as_str(), &self.path_prefix);
        debug!(
            "Query on {} with path_prefix={} => sub_path_exprs = {:?}",
            selector.path_expr, self.path_prefix, path_exprs
        );

        for path_expr in path_exprs {
            if path_expr.contains('*') {
                self.reply_with_matching_files(&query, path_expr).await;
            } else {
                // path_expr correspond to 1 single file. Read and send it.
                self.reply_with_file(&query, path_expr).await;
            }
        }

        Ok(())
    }
}

impl Drop for FileSystemStorage {
    fn drop(&mut self) {
        debug!("Closing InfluxDB storage");
        match self.on_closure {
            OnClosure::DeleteAll => {
                // TODO
            }
            OnClosure::DoNothing => {
                debug!(
                    "Close File System Storage, keeping directory {} as it is",
                    self.base_dir
                );
            }
        }
    }
}

// Scheduled removal of file meta-data
struct TimedFileCleanup {
    file: PathBuf,
}

#[async_trait]
impl Timed for TimedFileCleanup {
    async fn run(&mut self) {
        // TODO
    }
}

fn concat_str<S1: AsRef<str>, S2: AsRef<str>>(s1: S1, s2: S2) -> String {
    let mut result = String::with_capacity(s1.as_ref().len() + s2.as_ref().len());
    result.push_str(s1.as_ref());
    result.push_str(s2.as_ref());
    result
}
