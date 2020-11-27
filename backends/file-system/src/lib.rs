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
use std::borrow::Cow;
use std::convert::TryFrom;
use std::fs::DirBuilder;
use std::io::prelude::*;
use std::path::Path;
use std::path::PathBuf;
use std::time::{Duration, Instant};
use tempfile::tempfile_in;
use zenoh::net::{DataInfo, RBuf, Sample};
use zenoh::{Change, ChangeKind, Properties, Selector, Value, ZError, ZErrorKind, ZResult};
use zenoh_backend_traits::*;
use zenoh_util::collections::{Timed, TimedEvent, TimedHandle, Timer};
use zenoh_util::{zerror, zerror2};

mod data_info_mgt;
mod files_mgt;
use files_mgt::*;

// Properies used by the Backend
//  - None

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

        let on_closure = match props.get(PROP_STORAGE_ON_CLOSURE) {
            Some(s) => {
                if s == "delete_all" {
                    OnClosure::DeleteAll
                } else {
                    return zerror!(ZErrorKind::Other {
                        descr: format!("Unsupported value for 'on_closure' property: {}", s)
                    });
                }
            }
            None => OnClosure::DoNothing,
        };

        let base_dir = props
            .get(PROP_STORAGE_DIR)
            .map(|s| PathBuf::from(s))
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
                        r#"Cannot create File System Storage on "dir"={:?} : {}"#,
                        base_dir, err
                    )
                });
            }
        } else if !base_dir_path.is_dir() {
            return zerror!(ZErrorKind::Other {
                descr: format!(
                    r#"Cannot create File System Storage on "dir"={:?} : this is not a directory"#,
                    base_dir
                )
            });
        } else if let Err(err) = base_dir_path.read_dir() {
            return zerror!(ZErrorKind::Other {
                descr: format!(
                    r#"Cannot create File System Storage on "dir"={:?} : {}"#,
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
                            r#"Cannot create writeable File System Storage on "dir"={:?} : {}"#,
                            base_dir, err
                        )
                    })
                })?;
        }

        let files_mgr = FilesMgr::new(base_dir, follow_links, on_closure)?;

        let admin_status = zenoh::utils::properties_to_json_value(&props);
        Ok(Box::new(FileSystemStorage {
            admin_status,
            path_prefix,
            files_mgr,
            read_only,
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

struct FileSystemStorage {
    admin_status: Value,
    path_prefix: String,
    files_mgr: FilesMgr,
    read_only: bool,
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
        for path in self.files_mgr.matching_files(path_expr) {
            self.reply_with_file(query, path).await;
        }
    }

    async fn reply_with_file<P: AsRef<Path>>(&self, query: &Query, file: P) {
        debug!(
            "Replying to query on {} with file {:?}",
            query.res_name(),
            file.as_ref()
        );
        match self.files_mgr.read_file(&file) {
            Ok((content, encoding, timestamp)) => {
                // zenoh path for this file: convert to zpath and replace the base_dir prefix by path_prefix
                let relative_zpath =
                    &fspath_to_zpath(file.as_ref())[self.files_mgr.base_dir().as_os_str().len()..];
                let zpath = concat_str(&self.path_prefix, relative_zpath);

                let data_info = DataInfo {
                    source_id: None,
                    source_sn: None,
                    first_router_id: None,
                    first_router_sn: None,
                    timestamp: Some(timestamp),
                    kind: None,
                    encoding: Some(encoding),
                };
                query
                    .reply(Sample {
                        res_name: zpath,
                        payload: RBuf::from(content),
                        data_info: Some(data_info),
                    })
                    .await;
            }
            Err(e) => warn!(
                "Replying to query on {} : failed to read file {:?} : {}",
                query.res_name(),
                file.as_ref(),
                e
            ),
        }
    }
}

#[async_trait]
impl Storage for FileSystemStorage {
    async fn get_admin_status(&self) -> Value {
        self.admin_status.clone()
    }

    // When receiving a Sample (i.e. on PUT or DELETE operations)
    async fn on_sample(&mut self, sample: Sample) -> ZResult<()> {
        // transform the Sample into a Change to get kind, encoding and timestamp
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

                    // get latest timestamp for this file, it exists
                    // self.files_mgr.get_timestamp(file_path)

                    // encode the value as a string to be stored in InfluxDB

                    // Note: tags are stored as strings in InfluxDB, while fileds are typed.
                    // For simpler/faster deserialization, we store encoding, timestamp and base64 as fields.
                    // while the kind is stored as a tag to be indexed by InfluxDB and have faster queries on it.
                } else {
                    warn!(
                        "Received PUT for read-only Files System Storage on {:?} - ignored",
                        self.files_mgr.base_dir()
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
                        "Received DELETE for read-only Files System Storage on {:?} - ignored",
                        self.files_mgr.base_dir()
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
                // real path for this file is: base_dir + path_expr
                // let file = PathBuf::from(zpath_to_fspath(path_expr).as_ref());
                let fspath = zpath_to_fspath(path_expr);
                let file = concat_paths(&self.files_mgr.base_dir(), fspath);
                self.reply_with_file(&query, file).await;
            }
        }

        Ok(())
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

#[cfg(unix)]
#[inline(always)]
pub(crate) fn zpath_to_fspath(zpath: &str) -> Cow<'_, str> {
    Cow::from(zpath)
}

#[cfg(windows)]
pub(crate) fn zpath_to_fspath(zpath: &str) -> Cow<'_, str> {
    const WIN_SEP: &str = r#"\"#;
    Cow::from(zpath.replace('/', WIN_SEP))
}

#[cfg(unix)]
#[inline(always)]
pub(crate) fn fspath_to_zpath(fspath: &Path) -> Cow<'_, str> {
    fspath.to_string_lossy()
}

#[cfg(windows)]
pub(crate) fn fspath_to_zpath(fspath: &Path) -> Cow<'_, str> {
    const ZENOH_SEP: &str = "/";
    let cow = fspath.to_string_lossy();
    Cow::from(cow.replace(std::path::MAIN_SEPARATOR, ZENOH_SEP))
}

pub(crate) fn concat_paths<P1: AsRef<Path>, P2: AsRef<str>>(p1: P1, p2: P2) -> PathBuf {
    let mut os_str = p1.as_ref().as_os_str().to_os_string();
    os_str.push(p2.as_ref());
    PathBuf::from(os_str)
}
