//
// Copyright (c) 2024 ZettaScale Technology
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

use std::collections::{BTreeMap, HashMap};

use bloomfilter::Bloom;
use zenoh::{key_expr::OwnedKeyExpr, sample::SampleKind, time::Timestamp, Result};
use zenoh_backend_traits::config::ReplicaConfig;

use super::{
    classification::{EventRemoval, Interval, IntervalIdx},
    configuration::Configuration,
    digest::{Digest, Fingerprint},
};

/// An `Event` records the fact that a publication occurred on the associated key expression at the
/// associated timestamp.
///
/// When an `Event` is created, its [Fingerprint] is computed, using the `xxhash-rust` crate. This
/// [Fingerprint] is used to construct the [Digest] associated with the replication log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Event {
    maybe_stripped_key: Option<OwnedKeyExpr>,
    timestamp: Timestamp,
    action: SampleKind,
    fingerprint: Fingerprint,
}

impl Event {
    /// Creates a new [Event] with the provided key expression and timestamp.
    ///
    /// This function computes the [Fingerprint] of both using the `xxhash_rust` crate.
    pub fn new(key_expr: Option<OwnedKeyExpr>, timestamp: Timestamp, action: SampleKind) -> Self {
        let mut hasher = xxhash_rust::xxh3::Xxh3::default();
        if let Some(key_expr) = &key_expr {
            hasher.update(key_expr.as_bytes());
        }
        hasher.update(&timestamp.get_time().0.to_le_bytes());
        hasher.update(&timestamp.get_id().to_le_bytes());

        Self {
            maybe_stripped_key: key_expr,
            timestamp,
            action,
            fingerprint: hasher.digest().into(),
        }
    }

    /// Returns a reference over the key expression associated with this [Event].
    ///
    /// Note that this method can return `None` as the underlying key expression could be the
    /// *stripped* of a prefix.
    /// This prefix is defined as part of the configuration of the associated [Storage].
    pub fn key_expr(&self) -> &Option<OwnedKeyExpr> {
        &self.maybe_stripped_key
    }

    /// Returns the [Timestamp] associated with this [Event].
    //
    // NOTE: Even though `Timestamp` implements the `Copy` trait, it does not fit on two general
    //       purpose (64bits) registers so, in theory, a reference should be more efficient.
    //
    //       https://rust-lang.github.io/rust-clippy/master/#/trivially_copy_pass_by_ref
    pub fn timestamp(&self) -> &Timestamp {
        &self.timestamp
    }

    /// Returns the [Fingerprint] associated with this [Event].
    pub fn fingerprint(&self) -> Fingerprint {
        self.fingerprint
    }
}

/// The `EventInsertion` enumeration lists the possible outcomes when attempting to insert an
/// [Event] in the replication [Log].
///
/// The outcomes are:
/// - `New(Event)`: there was no [Event] in the log with the same key expression.
///
/// - `Replaced(Event)`: there was an [Event] in the log with the same key expression but an older
///   [Timestamp].
///
/// - `NotInsertedAsOlder`: there was an [Event] in the log with the same key expression but a more
///   recent [Timestamp].
///
/// - `NotInsertedAsOutOfBound`: the provided [Timestamp] is too far away in the future (compared to
///   the clock of this Zenoh node) and cannot be inserted in the log.
///
/// [Log]: LogLatest
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EventInsertion {
    New(Event),
    ReplacedOlder(Event),
    NotInsertedAsOlder,
    NotInsertedAsOutOfBound,
}

/// The `LogLatest` keeps track of the last publication that happened on a key expression.
///
/// By definition, the `LogLatest` is only compatible with storage that have the capability
/// `History::Latest` (the default value). For instance, that means that it will *not* work for
/// time-series storage that keep track of all the publications that happen for a given key
/// expression.
///
/// Internally, the `LogLatest` groups publications (i.e. [Event]s) according to their [Timestamp]
/// in [Interval]s and [SubInterval]s. The purpose of this grouping is to facilitate the alignment
/// and diminish the amount of data sent over the network. See the [Digest] structure for further
/// explanations.
///
/// As it only keeps track of the latest publication, whenever a new publication is received we need
/// to make sure that there is no [Event] with a newer [Timestamp] already present. The Bloom Filter
/// helps speed up this process by telling quickly if an event for that key expression is already
/// tracked or not.
///
/// [Interval]: super::classification::Interval
/// [SubInterval]: super::classification::SubInterval
pub struct LogLatest {
    pub(crate) configuration: Configuration,
    pub(crate) intervals: BTreeMap<IntervalIdx, Interval>,
    pub(crate) bloom_filter_event: Bloom<Option<OwnedKeyExpr>>,
}

impl LogLatest {
    /// Creates a new [LogLatest] configured with the provided [ReplicaConfig].
    pub fn new(
        storage_key_expr: OwnedKeyExpr,
        prefix: Option<OwnedKeyExpr>,
        replica_config: ReplicaConfig,
    ) -> Self {
        Self {
            configuration: Configuration::new(storage_key_expr, prefix, replica_config),
            intervals: BTreeMap::default(),
            // TODO Should these be configurable?
            //
            //      With their current values, the bloom filter structure will consume ~5MB. Note
            //      that this applies for each Storage that has replication enabled (hence, if a
            //      node has two Storage that have replication enabled, ~10MB of memory will be
            //      consumed on that node).
            //
            // 2 << 22 = 4_194_304 items.
            bloom_filter_event: Bloom::new_for_fp_rate(2 << 22, 0.01),
        }
    }

    /// Returns the [Configuration] associated with the [LogLatest].
    pub fn configuration(&self) -> &Configuration {
        &self.configuration
    }

    /// Lookup the provided key expression and, if found, return its associated [Event].
    pub fn lookup(&self, stripped_key: &Option<OwnedKeyExpr>) -> Option<&Event> {
        if !self.bloom_filter_event.check(stripped_key) {
            return None;
        }

        for interval in self.intervals.values().rev() {
            if let Some(event) = interval.lookup(stripped_key) {
                return Some(event);
            }
        }

        None
    }

    /// Attempts to insert the provided [Event] in the replication log and return the [Insertion]
    /// outcome.
    ///
    /// # Caveat: out of bound
    ///
    /// This method will record an error in the Zenoh log if the timestamp associated with the
    /// [Event] is so far in the future that the index of its interval is higher than
    /// [u64::MAX]. This should not happen unless a specially crafted [Event] is sent to this node
    /// or if the internal clock of the host that produced it is (very) far in the future.
    pub(crate) fn insert_event(&mut self, event: Event) -> EventInsertion {
        let mut result = None;

        // A Bloom filter never returns false negative. Hence if the call to `check_and_set` we
        // can be sure (provided that we update correctly the Bloom filter) that there is no
        // Event with that key expression.
        if self.bloom_filter_event.check(event.key_expr()) {
            // The Bloom filter indicates that there is an Event with the same key expression,
            // we need to check if it is older or not than the one we are processing.
            //
            // By construction of the LogLatest, there can only be a single [Event] with the
            // same key expression, hence the moment we find it we can skip the search.
            //
            // NOTE: `rev()`
            //       We are making here the following assumption: it is more likely that a recent
            //       key will be updated. Iterating over a `BTreeMap` will yield its elements in
            //       increasing order --- in our particular case that means from oldest to
            //       newest. Using `rev()` yields them from newest to oldest.
            for interval in self.intervals.values_mut().rev() {
                match interval.if_newer_remove_older(event.key_expr(), event.timestamp()) {
                    EventRemoval::RemovedOlder(old_event) => {
                        result = Some(old_event);
                        break;
                    }
                    EventRemoval::KeptNewer => return EventInsertion::NotInsertedAsOlder,
                    EventRemoval::NotFound => continue,
                }
            }
        }

        let result = match result {
            Some(old_event) => EventInsertion::ReplacedOlder(old_event),
            None => EventInsertion::New(event.clone()),
        };

        let (interval_idx, sub_interval_idx) = match self
            .configuration
            .get_time_classification(event.timestamp())
        {
            Ok((interval_idx, sub_interval_idx)) => (interval_idx, sub_interval_idx),
            Err(e) => {
                tracing::error!("{e:?}");
                return EventInsertion::NotInsertedAsOutOfBound;
            }
        };

        self.bloom_filter_event.set(event.key_expr());

        self.intervals
            .entry(interval_idx)
            .or_default()
            .insert_unchecked(sub_interval_idx, event);

        result
    }

    /// Updates the replication log with the provided set of [Event]s and return the updated
    /// [Digest].
    ///
    /// # Caveat: out of bounds [Event]s
    ///
    /// This method will log an error message for all [Event]s that have a [Timestamp] that is so
    /// far in the future that the index of their interval is higher than [u64::MAX]. This should
    /// not happen unless specifically crafted [Event]s are sent to this node or if the internal
    /// clock of a host is (very) far in the future.
    pub fn update(&mut self, events: impl Iterator<Item = Event>) {
        events.for_each(|event| {
            self.insert_event(event);
        });
    }

    /// Retrieves the latest [Digest], assuming that the hot era starts at the last elapsed
    /// interval.
    ///
    /// # Errors
    ///
    /// This method will return an error if the index of the last elapsed interval is superior to
    /// [u64::MAX]. In theory, this should not happen but if it does, **it is an error that cannot
    /// be recovered from (⚠️)**.
    pub fn digest(&self) -> Result<Digest> {
        let last_elapsed_interval = self.configuration.last_elapsed_interval()?;

        Ok(self.digest_from(last_elapsed_interval))
    }

    /// Considering the upper bound of the hot era, generates a [Digest] of the [LogLatest].
    ///
    /// Passing the upper bound of the hot era allows generating a [Digest] that can be compared
    /// with a possibly older [Digest].
    //
    // NOTE: One of the advantages of having that method take an upper bound is to facilitate unit
    //       testing.
    fn digest_from(&self, hot_era_upper_bound: IntervalIdx) -> Digest {
        let hot_era_lower_bound = self.configuration.hot_era_lower_bound(hot_era_upper_bound);
        let warm_era_lower_bound = self.configuration.warm_era_lower_bound(hot_era_upper_bound);

        let mut warm_era_fingerprints = HashMap::default();
        let mut hot_era_fingerprints = HashMap::default();
        let mut cold_era_fingerprint = Fingerprint::default();
        for (interval_idx, interval) in self
            .intervals
            .iter()
            .filter(|(&idx, _)| idx <= hot_era_upper_bound)
        {
            // NOTE: As the intervals are traversed in increasing order (because of the use of a
            //       [BTreeMap]) and as the cold era contains the most and older intervals
            //       (i.e. with a lower interval index), the order of the comparisons should
            //       minimise their number to generate the Digest.
            if *interval_idx < warm_era_lower_bound {
                cold_era_fingerprint ^= interval.fingerprint();
            } else if *interval_idx < hot_era_lower_bound {
                if interval.fingerprint() != Fingerprint::default() {
                    warm_era_fingerprints.insert(*interval_idx, interval.fingerprint());
                }
            } else {
                hot_era_fingerprints.insert(*interval_idx, interval.sub_intervals_fingerprints());
            }
        }

        Digest {
            configuration_fingerprint: self.configuration.fingerprint(),
            cold_era_fingerprint,
            warm_era_fingerprints,
            hot_era_fingerprints,
        }
    }
}

#[cfg(test)]
#[path = "tests/log.test.rs"]
mod tests;
