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
use async_std::sync::{Arc, RwLock};
use log::{debug, trace, warn};
use rocksdb::{Options, DB};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use zenoh::net::ZInt;
use zenoh::{Timestamp, TimestampID, ZError, ZErrorKind, ZResult};
use zenoh_util::{zerror, zerror2};

const DB_FILENAME: &str = ".zenoh_datainfo";

pub(crate) struct DataInfoMgr {
    db: DB,
}

impl DataInfoMgr {
    pub(crate) fn new(base_dir: &Path) -> ZResult<Self> {
        let mut backup_file = PathBuf::from(base_dir);
        backup_file.push(DB_FILENAME);
        let backup_file_display = backup_file.display();

        let db = DB::open_default(backup_file).unwrap();

        Ok(DataInfoMgr { db })
    }

    pub(crate) fn get_encoding_and_timestamp(
        &self,
        file: &str,
    ) -> ZResult<Option<(ZInt, Timestamp)>> {
        Ok(None)
    }

    pub(crate) fn get_timestamp(&self, file: &str) -> ZResult<Option<Timestamp>> {
        Ok(None)
    }

    /*
    pub(crate) async fn put_data_info(
        &self,
        path: &str,
        encoding: ZInt,
        timestamp: Timestamp,
    ) -> ZResult<()> {
        let _ = self
            .mem_db
            .write()
            .await
            .prepare_cached(INSERT_STMT)
            .and_then(|mut stmt| {
                stmt.execute(params![path, false, encoding as i64, timestamp.to_string()])
            })
            .map_err(|e| {
                zerror2!(ZErrorKind::Other {
                    descr: format!("Failed to insert data-info for {} in db: {}", path, e)
                })
            })?;
        Ok(())
    }

    pub(crate) fn mark_data_info_deleted(&self, path: &str, timestamp: Timestamp) -> ZResult<()> {
        let _ = self
            .mem_db
            .prepare_cached(INSERT_STMT)
            .and_then(|mut stmt| stmt.execute(params![path, true, 0, timestamp.to_string()]))
            .map_err(|e| {
                zerror2!(ZErrorKind::Other {
                    descr: format!(
                        "Failed to mark data-info as deleted for {} in db: {}",
                        path, e
                    )
                })
            })?;
        Ok(())
    }

    pub(crate) fn delete_data_info(&self, path: &str) -> ZResult<()> {
        let _ = self
            .mem_db
            .prepare_cached(DELETE_STMT)
            .and_then(|mut stmt| stmt.execute(params![path]))
            .map_err(|e| {
                zerror2!(ZErrorKind::Other {
                    descr: format!("Failed to delete data-info for {} in db: {}", path, e)
                })
            })?;
        Ok(())
    }

    pub(crate) fn get_timestamp(&self, path: &str) -> ZResult<Option<Timestamp>> {
        self.mem_db
            .prepare_cached(SELECT_TS_STMT)
            .and_then(|mut stmt| {
                stmt.query_row(params![path], |row| {
                    row.get::<usize, TsWrapper>(0).map(|w| w.ts)
                })
                .optional()
            })
            .map_err(|e| {
                zerror2!(ZErrorKind::Other {
                    descr: format!(
                        "Failed to get timestamp for data-info for {} in db: {}",
                        path, e
                    )
                })
            })
    }*/
}
