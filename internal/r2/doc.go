// Package r2 owns publisher claims and the narrowly allow-listed immutable
// object transfer boundary to Cloudflare R2.
//
// Raw-day publication uses the source/symbol scope plus publisher-epoch lock and its
// SQLite journal. Replay-derivative publication uses a sealed bundle,
// bounded observations, pure reconciliation, narrow actions, diagnostic
// events, and a receipt; it does not use SQLite or stage state as authority.
// ObjectBackend.Open is the streaming read boundary used to hash existing
// derivative data without retaining the complete object in memory.
package r2
