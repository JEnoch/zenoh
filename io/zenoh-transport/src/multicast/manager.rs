//
// Copyright (c) 2023 ZettaScale Technology
//
// This program and the accompanying materials are made available under the
// terms of the Eclipse Public License 2.0 which is available at
// http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
// which is available at https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
//
// Contributors:
//   ZettaScale Zenoh Team, <zenoh@zettascale.tech>
//
use std::{collections::HashMap, sync::Arc, time::Duration};

use tokio::sync::Mutex;
#[cfg(feature = "transport_compression")]
use zenoh_config::CompressionMulticastConf;
#[cfg(feature = "shared-memory")]
use zenoh_config::ShmConf;
use zenoh_config::{Config, LinkTxConf};
use zenoh_core::zasynclock;
use zenoh_link::*;
use zenoh_protocol::{
    core::{parameters, ZenohIdProto},
    transport::close,
};
use zenoh_result::{bail, zerror, ZResult};

use crate::{
    multicast::{transport::TransportMulticastInner, TransportMulticast},
    TransportManager,
};

pub struct TransportManagerConfigMulticast {
    pub lease: Duration,
    pub keep_alive: usize,
    pub join_interval: Duration,
    pub max_sessions: usize,
    pub is_qos: bool,
    #[cfg(feature = "shared-memory")]
    pub is_shm: bool,
    #[cfg(feature = "transport_compression")]
    pub is_compression: bool,
}

pub struct TransportManagerBuilderMulticast {
    lease: Duration,
    keep_alive: usize,
    join_interval: Duration,
    max_sessions: usize,
    is_qos: bool,
    #[cfg(feature = "shared-memory")]
    is_shm: bool,
    #[cfg(feature = "transport_compression")]
    is_compression: bool,
}

pub struct TransportManagerStateMulticast {
    // Established listeners
    pub(crate) protocols: Arc<Mutex<HashMap<String, LinkManagerMulticast>>>,
    // Established transports
    pub(crate) transports: Arc<Mutex<HashMap<Locator, Arc<TransportMulticastInner>>>>,
}

pub struct TransportManagerParamsMulticast {
    pub config: TransportManagerConfigMulticast,
    pub state: TransportManagerStateMulticast,
}

impl TransportManagerBuilderMulticast {
    pub fn lease(mut self, lease: Duration) -> Self {
        self.lease = lease;
        self
    }

    pub fn keep_alive(mut self, keep_alive: usize) -> Self {
        self.keep_alive = keep_alive;
        self
    }

    pub fn join_interval(mut self, join_interval: Duration) -> Self {
        self.join_interval = join_interval;
        self
    }

    pub fn max_sessions(mut self, max_sessions: usize) -> Self {
        self.max_sessions = max_sessions;
        self
    }

    pub fn qos(mut self, is_qos: bool) -> Self {
        self.is_qos = is_qos;
        self
    }

    #[cfg(feature = "shared-memory")]
    pub fn shm(mut self, is_shm: bool) -> Self {
        self.is_shm = is_shm;
        self
    }

    #[cfg(feature = "transport_compression")]
    pub fn compression(mut self, is_compression: bool) -> Self {
        self.is_compression = is_compression;
        self
    }

    pub fn from_config(mut self, config: &Config) -> ZResult<TransportManagerBuilderMulticast> {
        self = self.lease(Duration::from_millis(
            *config.transport().link().tx().lease(),
        ));
        self = self.keep_alive(*config.transport().link().tx().keep_alive());
        self = self.join_interval(Duration::from_millis(
            config.transport().multicast().join_interval().unwrap(),
        ));
        self = self.max_sessions(config.transport().multicast().max_sessions().unwrap());
        self = self.qos(*config.transport().multicast().qos().enabled());
        #[cfg(feature = "shared-memory")]
        {
            self = self.shm(*config.transport().shared_memory().enabled());
        }

        Ok(self)
    }

    pub fn build(self) -> ZResult<TransportManagerParamsMulticast> {
        let config = TransportManagerConfigMulticast {
            lease: self.lease,
            keep_alive: self.keep_alive,
            join_interval: self.join_interval,
            max_sessions: self.max_sessions,
            is_qos: self.is_qos,
            #[cfg(feature = "shared-memory")]
            is_shm: self.is_shm,
            #[cfg(feature = "transport_compression")]
            is_compression: self.is_compression,
        };

        let state = TransportManagerStateMulticast {
            protocols: Arc::new(Mutex::new(HashMap::new())),
            transports: Arc::new(Mutex::new(HashMap::new())),
        };

        let params = TransportManagerParamsMulticast { config, state };

        Ok(params)
    }
}

impl Default for TransportManagerBuilderMulticast {
    fn default() -> TransportManagerBuilderMulticast {
        let link_tx = LinkTxConf::default();
        #[cfg(feature = "shared-memory")]
        let shm = ShmConf::default();
        #[cfg(feature = "transport_compression")]
        let compression = CompressionMulticastConf::default();

        let tmb = TransportManagerBuilderMulticast {
            lease: Duration::from_millis(*link_tx.lease()),
            keep_alive: *link_tx.keep_alive(),
            join_interval: Duration::from_millis(0),
            max_sessions: 0,
            is_qos: false,
            #[cfg(feature = "shared-memory")]
            is_shm: *shm.enabled(),
            #[cfg(feature = "transport_compression")]
            is_compression: *compression.enabled(),
        };
        tmb.from_config(&Config::default()).unwrap()
    }
}

impl TransportManager {
    pub fn config_multicast() -> TransportManagerBuilderMulticast {
        TransportManagerBuilderMulticast::default()
    }

    pub async fn close_multicast(&self) {
        tracing::trace!("TransportManagerMulticast::clear())");

        zasynclock!(self.state.multicast.protocols).clear();

        for (_, tm) in zasynclock!(self.state.multicast.transports).drain() {
            let _ = tm.close(close::reason::GENERIC).await;
        }
    }

    /*************************************/
    /*            LINK MANAGER           */
    /*************************************/
    async fn new_link_manager_multicast(&self, protocol: &str) -> ZResult<LinkManagerMulticast> {
        if !self.config.protocols.iter().any(|x| x.as_str() == protocol) {
            bail!(
                "Unsupported protocol: {}. Supported protocols are: {:?}",
                protocol,
                self.config.protocols
            );
        }

        let mut w_guard = zasynclock!(self.state.multicast.protocols);
        match w_guard.get(protocol) {
            Some(lm) => Ok(lm.clone()),
            None => {
                let lm = LinkManagerBuilderMulticast::make(protocol)?;
                w_guard.insert(protocol.to_string(), lm.clone());
                Ok(lm)
            }
        }
    }

    async fn del_link_manager_multicast(&self, protocol: &str) -> ZResult<()> {
        match zasynclock!(self.state.multicast.protocols).remove(protocol) {
            Some(_) => Ok(()),
            None => bail!(
                "Can not delete the link manager for protocol ({}) because it has not been found.",
                protocol
            ),
        }
    }

    /*************************************/
    /*             TRANSPORT             */
    /*************************************/
    pub async fn open_transport_multicast(
        &self,
        mut endpoint: EndPoint,
    ) -> ZResult<TransportMulticast> {
        let p = endpoint.protocol();
        if !self
            .config
            .protocols
            .iter()
            .any(|x| x.as_str() == p.as_str())
        {
            bail!(
                "Unsupported protocol: {}. Supported protocols are: {:?}",
                p,
                self.config.protocols
            );
        }
        if !self
            .locator_inspector
            .is_multicast(&endpoint.to_locator())
            .await?
        {
            bail!(
                "Can not open a multicast transport with a unicast endpoint: {}.",
                endpoint
            )
        }

        // Automatically create a new link manager for the protocol if it does not exist
        let manager = self
            .new_link_manager_multicast(endpoint.protocol().as_str())
            .await?;
        // Fill and merge the endpoint configuration
        if let Some(config) = self.config.endpoints.get(endpoint.protocol().as_str()) {
            let mut config = parameters::Parameters::from(config.as_str());
            // Overwrite config with current endpoint parameters
            config.extend_from_iter(endpoint.config().iter());
            endpoint = EndPoint::new(
                endpoint.protocol(),
                endpoint.address(),
                endpoint.metadata(),
                config.as_str(),
            )?;
        }

        // Open the link
        let link = manager.new_link(&endpoint).await?;
        super::establishment::open_link(self, link).await
    }

    pub async fn get_transport_multicast(&self, zid: &ZenohIdProto) -> Option<TransportMulticast> {
        for t in zasynclock!(self.state.multicast.transports).values() {
            if t.get_peers().iter().any(|p| p.zid == *zid) {
                return Some(t.into());
            }
        }
        None
    }

    pub async fn get_transports_multicast(&self) -> Vec<TransportMulticast> {
        zasynclock!(self.state.multicast.transports)
            .values()
            .map(|t| t.into())
            .collect()
    }

    pub(super) async fn del_transport_multicast(&self, locator: &Locator) -> ZResult<()> {
        let mut guard = zasynclock!(self.state.multicast.transports);
        let res = guard.remove(locator);

        if !guard
            .iter()
            .any(|(l, _)| l.protocol() == locator.protocol())
        {
            let _ = self
                .del_link_manager_multicast(locator.protocol().as_str())
                .await;
        }

        res.map(|_| ()).ok_or_else(|| {
            let e = zerror!("Can not delete the transport for locator: {}", locator);
            tracing::trace!("{}", e);
            e.into()
        })
    }

    /*************************************/
    /*              LISTENER             */
    /*************************************/
    pub async fn add_listener_multicast(&self, endpoint: EndPoint) -> ZResult<Locator> {
        let locator = endpoint.to_locator().to_owned();
        self.open_transport_multicast(endpoint).await?;
        Ok(locator)
    }

    pub async fn del_listener_multicast(&self, endpoint: &EndPoint) -> ZResult<()> {
        let locator = endpoint.to_locator();

        let mut guard = zasynclock!(self.state.multicast.transports);
        let res = guard.remove(&locator);

        if !guard
            .iter()
            .any(|(l, _)| l.protocol() == locator.protocol())
        {
            let _ = self
                .del_link_manager_multicast(locator.protocol().as_str())
                .await;
        }

        res.map(|_| ()).ok_or_else(|| {
            let e = zerror!("Can not delete the transport for locator: {}", locator);
            tracing::trace!("{}", e);
            e.into()
        })
    }

    pub async fn get_listeners_multicast(&self) -> Vec<EndPoint> {
        zasynclock!(self.state.multicast.transports)
            .values()
            .map(|t| t.locator.clone().into())
            .collect()
    }

    pub async fn get_locators_multicast(&self) -> Vec<Locator> {
        zasynclock!(self.state.multicast.transports)
            .values()
            .map(|t| t.locator.clone())
            .collect()
    }
}
