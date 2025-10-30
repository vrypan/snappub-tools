Command: `snappub <url> comments <subcommand>`

Subcommands:
- `sync` - Fetch and sync castAdd/castRemove messages from snapchain blocks
- `export` - Export synced data to JSON files

This command maintains a local database of comment threads under a specific URL.

## API Usage

Uses HubService.GetShardChunks() from vrypan/farcaster-go/farcaster to query a Farcaster node.
- Processes shards 1 and 2 (user data is sharded by (FID % num_shards) + 1)
- Calls GetShardChunks with ShardChunksRequest{ shard_id, start_block_number, stop_block_number }
- Returns ShardChunksResponse containing array of ShardChunk objects
- Each ShardChunk contains Transactions with user_messages (Message objects for castAdd/castRemove)
- Processes in batches of 100 chunks to avoid gRPC message size limits (4MB)

## Subcommand: sync

Usage: `snappub <url> comments sync [--start-block <block>]`

Flags:
- `--start-block <block>`: Block number to start from (default: 0)
  - `0`: Resume from saved state (last processed block + 1), or block 1 if first sync
  - Positive value (e.g., `1000`): Start from this specific block number
  - Negative value (e.g., `-100`): Start from current_block - N blocks

Processing Flow:
1. Get hub info to determine current block height for each shard
2. Calculate start block for each shard based on --start-block flag
3. Load last processed block number per shard from BadgerDB (if --start-block=0)
4. For each shard (1, 2):
   - Call GetBlocks(shard_id, start_block, nil)
   - Stream blocks until current height
   - Process each Transaction in block.transactions
   - For each user_message in transaction, apply filtering logic
   - Fetch/cache user metadata (author, parent cast fid, mentions) via GetUserDataByFid when cache entry is missing or stale
   - Save block number after processing each block
5. Batch mode: Exit after reaching current block height on all shards

## Filtering Logic

For each Message in Transaction.user_messages:

1. If MESSAGE_TYPE_CAST_ADD:
   - Extract castAddBody.parent_url
   - If parent_url starts with <url>:
     → Store cast and mark parent_url as tracked
   - Else if parent_url exists in DB as tracked:
     → Store cast (reply to tracked URL thread)
   - Else if castAddBody.parent_cast_id.hash exists in DB:
     → Store cast (reply to tracked cast)
   - Else:
     → Skip

2. If MESSAGE_TYPE_CAST_REMOVE:
   - If cast hash exists in DB:
     → Store the remove message (will be used to mark cast as removed during export)

## BadgerDB Schema

Database location: ~/.config/snappub/comments.db/

All keys prefix with ASCII strings and then append raw data directly (no hex encoding). Farcaster hashes are stored as their raw 20-byte values.

Key-Value pairs (all values are raw protobuf bytes unless noted):

### Cast Storage
- Key: `cast:<hash>`
- Value: Message protobuf bytes (raw)
- Purpose: Store the actual cast message, lookup by hash
- Note: `<hash>` is the raw 20-byte hash immediately appended after `cast:`

### URL Tracking
- Key: `url:<parentUrl>`
- Value: empty byte (marker only)
- Purpose: Fast lookup to check if we're tracking this parentUrl

### URL Index (for export)
- Key: `url_idx:<parentUrl>:<block_be>:<event_idx_be>:<hash>`
- Value: empty byte (index only)
- Purpose: List all casts under a parentUrl in block order
- Note: block_be is 8-byte big-endian uint64 (block number)
- Note: event_idx_be is 8-byte big-endian uint64 (event index within block)
- Note: `<hash>` is the raw cast hash bytes appended after the final colon
- Rationale: Block order ensures causal ordering (removals come after adds)

### Parent Cast Index (for building threads)
- Key: `parent_idx:<parentCastHash>:<block_be>:<event_idx_be>:<hash>`
- Value: empty byte (index only)
- Purpose: Find all replies to a specific cast in block order
- Note: block_be is 8-byte big-endian uint64 (block number)
- Note: event_idx_be is 8-byte big-endian uint64 (event index within block)
- Note: `<parentCastHash>` and `<hash>` are raw hash bytes appended directly

### State Tracking
- Key: `state:shard:<shard_id>`
- Value: uint64 as 8-byte big-endian (block number)
- Purpose: Resume processing from last block per shard

### Cast Remove Index
- Key: `remove:<hash>`
- Value: Message protobuf bytes (the remove message)
- Purpose: Mark casts as removed during export
- Note: `<hash>` is the raw hash bytes appended after `remove:`

### User Metadata Cache
- Key: `user:<fid>`
- Value: JSON blob `{ fid, username, displayName, avatar, updatedAt }`
- Purpose: Cache HubService.GetUserDataByFid responses to avoid repeated gRPC lookups during sync/export
- Refresh policy: Entries older than the configured TTL (`userCacheTTL`, currently 6h) trigger a fresh fetch

## Subcommand: export

Usage: `snappub <url> comments export`

Behavior:
1. Scan all keys with prefix `url:<url>` to find tracked URLs matching <url> prefix
2. For each tracked URL:
   - Create JSON file: <sanitized_url>.json
   - Query `url_idx:<url>:*` to get root-level casts (in block order)
   - For each root cast:
     - Load Message from `cast:<hash>`
     - Check if `remove:<hash>` exists (marked as removed)
     - Recursively load replies via `parent_idx:<hash>:*`
     - Build nested JSON structure
     - Record all encountered FIDs (authors, mentions, embed castFIDs, parent casts)
   - Load user metadata for recorded FIDs from cache (refreshing stale entries via GetUserDataByFid as needed)
   - Sort output by Message.data.timestamp for chronological presentation

JSON Output Format:
```json
{
  "parentUrl": "https://example.com/post/123",
  "lastUpdated": "2025-10-30T12:34:56Z",
  "rootCasts": [
    {
      "hash": "0x...",
      "fid": 123,
      "timestamp": "2025-10-30T12:34:56Z",
      "text": "...",
      "embeds": [...],
      "mentions": [...],
      "removed": false,
      "replies": [
        {
          "hash": "0x...",
          "fid": 456,
          "timestamp": "2025-10-30T12:35:00Z",
          "text": "...",
          "removed": false,
          "replies": [...]
        }
      ]
    }
  ],
  "users": {
    "123": {
      "username": "username123",
      "displayName": "Display Name",
      "avatar": "https://..."
    }
  }
}
```
## Future Enhancements

- Continuous mode: Keep streaming new blocks in real-time (not in initial implementation)
- Multiple URL tracking: Support syncing multiple URL prefixes simultaneously
