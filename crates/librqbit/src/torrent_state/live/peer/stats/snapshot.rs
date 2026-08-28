use std::{collections::HashMap, sync::atomic::Ordering};

use serde::{Deserialize, Serialize};

use crate::{
    stream_connect::ConnectionKind,
    torrent_state::live::peer::{Peer, PeerState},
};

#[derive(Serialize, Deserialize)]
pub struct PeerCounters {
    pub incoming_connections: u32,
    pub fetched_bytes: u64,
    pub uploaded_bytes: u64,
    pub total_time_connecting_ms: u64,
    pub connection_attempts: u32,
    pub connections: u32,
    pub errors: u32,
    pub fetched_chunks: u32,
    pub downloaded_and_checked_pieces: u32,
    pub total_piece_download_ms: u64,
    pub times_stolen_from_me: u32,
    pub times_i_stole: u32,
}

#[derive(Serialize)]
pub struct PeerStats {
    pub counters: PeerCounters,
    pub state: &'static str,
    pub conn_kind: Option<ConnectionKind>,
    pub client_name: Option<String>,
    /// How many pieces this peer has, from the bitfield tracked for piece
    /// picking. `None` when the peer is not live and so has no bitfield.
    ///
    /// Lets an embedder compute piece availability across the swarm — the
    /// rarest-piece copy count — which is otherwise not derivable from the
    /// public API. `counters.downloaded_and_checked_pieces` answers a
    /// different question: how many pieces this peer sent *us*.
    pub have_pieces: Option<u32>,
    /// The peer's bitfield, as the bytes it sent.
    ///
    /// `None` unless [`PeerStatsFilter::include_bitfield`] was set and the
    /// peer is live. Trailing bits past `total_pieces` are spare and must be
    /// ignored — see [`PeerStats::from_peer`].
    pub have_bitfield: Option<Vec<u8>>,
}

impl PeerStats {
    /// Builds a snapshot for one peer.
    ///
    /// `total_pieces` is needed because a peer's bitfield is stored as the
    /// bytes it sent, and a bitfield is byte-padded: the trailing bits past
    /// the last real piece are spare. The spec says a peer must zero them,
    /// but `on_bitfield` only validates the byte *length*, so a peer that
    /// sets them would otherwise inflate the count by up to 7.
    pub(crate) fn from_peer(peer: &Peer, total_pieces: u32, include_bitfield: bool) -> Self {
        let state = peer.get_state();
        Self {
            counters: peer.stats.counters.as_ref().into(),
            state: state.name(),
            conn_kind: match state {
                PeerState::Live(l) => Some(l.connection_kind),
                _ => None,
            },
            client_name: match state {
                PeerState::Live(l) => l.client_name.clone(),
                _ => None,
            },
            have_pieces: match state {
                PeerState::Live(l) => Some(count_have_pieces(&l.bitfield, total_pieces)),
                _ => None,
            },
            have_bitfield: match state {
                PeerState::Live(l) if include_bitfield => Some(l.bitfield.as_raw_slice().to_vec()),
                _ => None,
            },
        }
    }
}

/// Counts the pieces a peer claims, ignoring the bitfield's spare trailing
/// bits.
///
/// See the note on [`PeerStats::from_peer`] for why the raw
/// `bitfield.count_ones()` is not correct here. The `min` guards the slice:
/// `on_bitfield` rejects a bitfield of the wrong byte length, so the two
/// should agree, but a panic here would take down a stats call.
fn count_have_pieces(bitfield: &crate::type_aliases::BF, total_pieces: u32) -> u32 {
    let end = (total_pieces as usize).min(bitfield.len());
    bitfield[..end].count_ones() as u32
}

#[cfg(test)]
mod tests {
    use super::count_have_pieces;
    use crate::type_aliases::BF;

    /// A peer that sets the spare trailing bits must not inflate the count.
    ///
    /// The spec says those bits are zero, but `on_bitfield` only validates the
    /// byte length, so this is reachable from the wire.
    #[test]
    fn ignores_padding_bits() {
        // 10 pieces needs 2 bytes, leaving 6 spare bits. Claim 3 real pieces
        // and then set every spare bit.
        let bf = BF::from_boxed_slice(vec![0b1110_0000, 0b0011_1111].into_boxed_slice());
        assert_eq!(count_have_pieces(&bf, 10), 3);
        // Counting the whole bitfield is what we are guarding against.
        assert_eq!(bf.count_ones(), 9);
    }

    #[test]
    fn counts_a_seed_as_every_piece() {
        let bf = BF::from_boxed_slice(vec![0xff, 0xff].into_boxed_slice());
        assert_eq!(count_have_pieces(&bf, 10), 10);
    }

    #[test]
    fn counts_an_empty_peer_as_none() {
        let bf = BF::from_boxed_slice(vec![0x00, 0x00].into_boxed_slice());
        assert_eq!(count_have_pieces(&bf, 10), 0);
    }
}

impl From<&super::atomic::PeerCountersAtomic> for PeerCounters {
    fn from(counters: &super::atomic::PeerCountersAtomic) -> Self {
        Self {
            incoming_connections: counters.incoming_connections.load(Ordering::Relaxed),
            fetched_bytes: counters.fetched_bytes.load(Ordering::Relaxed),
            uploaded_bytes: counters.uploaded_bytes.load(Ordering::Relaxed),
            total_time_connecting_ms: counters.total_time_connecting_ms.load(Ordering::Relaxed),
            connection_attempts: counters
                .outgoing_connection_attempts
                .load(Ordering::Relaxed),
            connections: counters.outgoing_connections.load(Ordering::Relaxed),
            errors: counters.errors.load(Ordering::Relaxed),
            fetched_chunks: counters.fetched_chunks.load(Ordering::Relaxed),
            downloaded_and_checked_pieces: counters
                .downloaded_and_checked_pieces
                .load(Ordering::Relaxed),
            total_piece_download_ms: counters.total_piece_download_ms.load(Ordering::Relaxed),
            times_i_stole: counters.times_i_stole.load(Ordering::Relaxed),
            times_stolen_from_me: counters.times_stolen_from_me.load(Ordering::Relaxed),
        }
    }
}

#[derive(Serialize)]
pub struct PeerStatsSnapshot {
    pub peers: HashMap<String, PeerStats>,
}

#[derive(Clone, Copy, Default, Deserialize)]
pub enum PeerStatsFilterState {
    #[serde(rename = "all")]
    All,
    #[default]
    #[serde(rename = "live")]
    Live,
}

impl PeerStatsFilterState {
    pub(crate) fn matches(&self, s: &PeerState) -> bool {
        matches!((self, s), (Self::All, _) | (Self::Live, PeerState::Live(_)))
    }
}

#[derive(Default, Deserialize)]
pub struct PeerStatsFilter {
    #[serde(default)]
    pub state: PeerStatsFilterState,
    /// Include each peer's raw bitfield in `have_bitfield`.
    ///
    /// Off by default: it costs a byte per eight pieces per peer, which is
    /// waste for the many callers that only want `have_pieces`. Turn it on to
    /// compute per-piece availability across the swarm, which needs to know
    /// *which* pieces each peer holds rather than how many.
    #[serde(default)]
    pub include_bitfield: bool,
}
