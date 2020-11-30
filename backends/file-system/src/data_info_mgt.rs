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
use log::trace;
use rocksdb::DB;
use std::convert::TryFrom;
use std::io::Write;
use std::path::{Path, PathBuf};
use uhlc::NTP64;
use zenoh::net::ZInt;
use zenoh::{Timestamp, ZError, ZErrorKind, ZResult};
use zenoh_util::{zerror, zerror2};

const DB_FILENAME: &str = ".zenoh_datainfo";

// size of serialized data-info: encoding (u64) + timestamp (u64 + ID)
const VAL_LEN: usize = 8 + 8 + uhlc::ID::MAX_SIZE;

pub(crate) struct DataInfoMgr {
    db: DB,
}

impl DataInfoMgr {
    pub(crate) fn new(base_dir: &Path) -> ZResult<Self> {
        let mut backup_file = PathBuf::from(base_dir);
        backup_file.push(DB_FILENAME);

        let db = DB::open_default(&backup_file).map_err(|e| {
            zerror2!(ZErrorKind::Other {
                descr: format!(
                    "Failed to open data-info database from {:?}: {}",
                    backup_file, e
                )
            })
        })?;

        Ok(DataInfoMgr { db })
    }

    pub(crate) fn put_data_info<P: AsRef<Path>>(
        &self,
        file: P,
        encoding: ZInt,
        timestamp: Timestamp,
    ) -> ZResult<()> {
        let key = file.as_ref().to_string_lossy();
        trace!("Put data-info for {}", key);
        let mut value: Vec<u8> = Vec::with_capacity(VAL_LEN);
        value
            .write_all(&encoding.to_ne_bytes())
            .and_then(|()| value.write_all(&timestamp.get_time().as_u64().to_ne_bytes()))
            .and_then(|()| value.write_all(timestamp.get_id().as_slice()))
            .map_err(|e| {
                zerror2!(ZErrorKind::Other {
                    descr: format!("Failed to encode data-info for {:?}: {}", file.as_ref(), e)
                })
            })?;

        self.db.put(key.as_bytes(), value).map_err(|e| {
            zerror2!(ZErrorKind::Other {
                descr: format!("Failed to save data-info for {:?}: {}", file.as_ref(), e)
            })
        })
    }

    pub(crate) fn get_encoding_and_timestamp<P: AsRef<Path>>(
        &self,
        file: P,
    ) -> ZResult<Option<(ZInt, Timestamp)>> {
        let key = file.as_ref().to_string_lossy();
        trace!("Get data-info for {}", key);
        match self.db.get_pinned(key.as_bytes()) {
            Ok(Some(pin_val)) => {
                let mut encoding_bytes = [0u8; 8];
                encoding_bytes.clone_from_slice(&pin_val.as_ref()[..8]);
                let encoding = ZInt::from_ne_bytes(encoding_bytes);
                let mut time_bytes = [0u8; 8];
                time_bytes.clone_from_slice(&pin_val.as_ref()[8..16]);
                let time = u64::from_ne_bytes(time_bytes);
                let id = uhlc::ID::try_from(&pin_val.as_ref()[16..]).unwrap();
                let timestamp = Timestamp::new(NTP64(time), id);

                Ok(Some((encoding, timestamp)))
            }
            Ok(None) => {
                trace!("data-info for {:?} not found", file.as_ref());
                Ok(None)
            }
            Err(e) => zerror!(ZErrorKind::Other {
                descr: format!("Failed to save data-info for {:?}: {}", file.as_ref(), e)
            }),
        }
    }

    pub(crate) fn get_timestamp<P: AsRef<Path>>(&self, file: P) -> ZResult<Option<Timestamp>> {
        let key = file.as_ref().to_string_lossy();
        trace!("Get timestamp for {}", key);
        match self.db.get_pinned(key.as_bytes()) {
            Ok(Some(pin_val)) => {
                let mut time_bytes = [0u8; 8];
                time_bytes.clone_from_slice(&pin_val.as_ref()[8..16]);
                let time = u64::from_ne_bytes(time_bytes);
                let id = uhlc::ID::try_from(&pin_val.as_ref()[16..]).unwrap();
                let timestamp = Timestamp::new(NTP64(time), id);

                Ok(Some(timestamp))
            }
            Ok(None) => {
                trace!("timestamp for {:?} not found", file.as_ref());
                Ok(None)
            }
            Err(e) => zerror!(ZErrorKind::Other {
                descr: format!("Failed to save data-info for {:?}: {}", file.as_ref(), e)
            }),
        }
    }

    /*
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
    }*/
}
