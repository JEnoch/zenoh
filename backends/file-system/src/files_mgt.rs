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

use log::{debug, warn};
use std::fs::{metadata, DirBuilder, File};
use std::io::prelude::*;
use std::iter::Iterator;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use walkdir::{IntoIter, WalkDir};
use zenoh::net::utils::resource_name;
use zenoh::net::{encoding, ZInt};
use zenoh::{Timestamp, TimestampID, ZError, ZErrorKind, ZResult};
use zenoh_util::{zerror, zerror2};

use crate::data_info_mgt::*;

pub(crate) enum OnClosure {
    DeleteAll,
    DoNothing,
}

pub(crate) struct FilesMgr {
    base_dir: PathBuf,
    data_info_mgr: DataInfoMgr,
    follow_links: bool,
    dir_builder: DirBuilder,
    on_closure: OnClosure,
}

impl FilesMgr {
    pub(crate) fn new(
        base_dir: PathBuf,
        follow_links: bool,
        on_closure: OnClosure,
    ) -> ZResult<Self> {
        let data_info_mgr = DataInfoMgr::new(base_dir.as_path())?;

        let mut dir_builder = DirBuilder::new();
        dir_builder.recursive(true);

        Ok(FilesMgr {
            base_dir: base_dir,
            data_info_mgr,
            follow_links,
            dir_builder,
            on_closure,
        })
    }

    pub(crate) fn base_dir(&self) -> &Path {
        &self.base_dir.as_path()
    }

    // Search for files matching path_expr.
    // WARNING: the returend paths will include the "base_dir" prefix (because of Walkdir behaviour)
    pub(crate) fn matching_files<'a>(&self, path_expr: &'a str) -> FilesIterator<'a> {
        // find the longest segment without '*' to search for files only in the corresponding
        let star_idx = path_expr.find('*').unwrap();
        let segment = match path_expr[..star_idx].rfind('/') {
            Some(i) => &path_expr[..i],
            None => "",
        };
        let mut search_dir = self.base_dir.as_os_str().to_os_string();
        search_dir.push(segment);
        let base_dir_len = self.base_dir.as_os_str().len();

        debug!(
            "For path_expr={} search matching files in {:?}",
            path_expr, search_dir
        );
        let walkdir = WalkDir::new(search_dir).follow_links(self.follow_links);
        FilesIterator {
            walk_iter: walkdir.into_iter(),
            path_expr,
            base_dir_len,
        }
    }

    // Read a file and return it's content (as Vec<u8>), encoding and timestamp.
    // Encoding and timestamp are retrieved from the data_info_mgr if file was put via zenoh.
    // Otherwise, the encoding is guessed from the file extension, and the timestamp is computed from the file's time.
    // WARNING: 'file' is expected to include the "base_dir" prefix (to be callable with results of matching_files())
    pub(crate) fn read_file<P: AsRef<Path>>(&self, file: P) -> ZResult<(Vec<u8>, ZInt, Timestamp)> {
        match File::open(&file) {
            Ok(mut f) => {
                // TODO: what if file is too big ??
                let size = f.metadata().map(|m| m.len()).unwrap_or(256);
                if size <= usize::MAX as u64 {
                    let mut content: Vec<u8> = Vec::with_capacity(size as usize);
                    if let Err(e) = f.read_to_end(&mut content) {
                        zerror!(ZErrorKind::Other {
                            descr: format!(r#"Error reading file {:?}: {}"#, file.as_ref(), e)
                        })
                    } else {
                        let (timestamp, encoding) =
                            self.get_encoding_and_timestamp(file.as_ref().to_str().unwrap())?;
                        Ok((content, timestamp, encoding))
                    }
                } else {
                    zerror!(ZErrorKind::Other {
                        descr: format!(
                            r#"Error reading file {:?}: too big to fit in memory"#,
                            file.as_ref()
                        )
                    })
                }
            }
            Err(e) => zerror!(ZErrorKind::Other {
                descr: format!(r#"Error reading file {:?}: {}"#, file.as_ref(), e)
            }),
        }
    }

    fn get_encoding_and_timestamp(&self, file: &str) -> ZResult<(ZInt, Timestamp)> {
        // TODO: try to get Timestamp and encoding from meta-data file
        match self.data_info_mgr.get_encoding_and_timestamp(file)? {
            Some(x) => Ok(x),
            None => {
                // fallback: guess mime type from file extension
                let mime_type = mime_guess::from_path(file).first_or_octet_stream();
                debug!("Found mime-type {} for {}", mime_type, file);
                let encoding = encoding::from_str(mime_type.essence_str())
                    .unwrap_or(encoding::APP_OCTET_STREAM);

                // fallback: get timestamp from file's metadata
                let timestamp = self.get_timestamp_from_metadata(file)?;

                Ok((encoding, timestamp))
            }
        }
    }

    pub(crate) fn get_timestamp(&self, file: &str) -> ZResult<Option<Timestamp>> {
        // try to get Timestamp from data_info_mgr
        match self.data_info_mgr.get_timestamp(file)? {
            Some(x) => Ok(Some(x)),
            None => {
                // fallback: get timestamp from file's metadata if it exists
                if Path::new(file).exists() {
                    let timestamp = self.get_timestamp_from_metadata(file)?;
                    Ok(Some(timestamp))
                } else {
                    Ok(None)
                }
            }
        }
    }

    fn get_timestamp_from_metadata(&self, file: &str) -> ZResult<Timestamp> {
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

impl Drop for FilesMgr {
    fn drop(&mut self) {
        debug!("Closing File System Storage on {:?}", self.base_dir);
        match self.on_closure {
            OnClosure::DeleteAll => {
                // TODO
            }
            OnClosure::DoNothing => {
                debug!(
                    "Close File System Storage, keeping directory {:?} as it is",
                    self.base_dir
                );
            }
        }
    }
}

pub(crate) struct FilesIterator<'a> {
    walk_iter: IntoIter,
    path_expr: &'a str,
    base_dir_len: usize,
}

impl Iterator for FilesIterator<'_> {
    type Item = PathBuf;
    fn next(&mut self) -> Option<Self::Item> {
        let base_dir_len = self.base_dir_len;
        let path_expr = self.path_expr;
        self.walk_iter.find_map(|result| match result {
            Ok(e) => {
                if e.file_type().is_file() {
                    if let Some(p) = e.path().to_str() {
                        let shortpath = &p[base_dir_len..];
                        if resource_name::intersect(shortpath, path_expr) {
                            return Some(e.into_path());
                        }
                    }
                }
                None
            }
            Err(e) => {
                warn!("Error looking for file matching {} : {}", path_expr, e);
                None
            }
        })
    }
}

pub(crate) fn concat_str<S1: AsRef<str>, S2: AsRef<str>>(s1: S1, s2: S2) -> String {
    let mut result = String::with_capacity(s1.as_ref().len() + s2.as_ref().len());
    result.push_str(s1.as_ref());
    result.push_str(s2.as_ref());
    result
}
