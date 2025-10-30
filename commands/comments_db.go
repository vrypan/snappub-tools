package commands

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	pb "github.com/vrypan/farcaster-go/farcaster"
	"google.golang.org/protobuf/proto"
)

type CommentsDB struct {
	db   *badger.DB
	path string
}

// OpenCommentsDB opens or creates the comments database
func OpenCommentsDB() (*CommentsDB, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return nil, fmt.Errorf("failed to get home directory: %w", err)
	}

	dbPath := filepath.Join(home, ".config", "snappub", "comments.db")

	// Create directory if it doesn't exist
	if err := os.MkdirAll(dbPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create database directory: %w", err)
	}

	opts := badger.DefaultOptions(dbPath)
	opts.Logger = nil // Disable badger's verbose logging
	// Memory optimizations
	opts.MemTableSize = 64 << 20  // 64MB
	opts.BaseTableSize = 2 << 20  // 2MB
	opts.BaseLevelSize = 10 << 20 // 10MB
	opts.LevelSizeMultiplier = 10
	opts.TableSizeMultiplier = 2
	opts.MaxLevels = 7
	opts.ValueLogFileSize = 64 << 20 // 64MB
	opts.ValueLogMaxEntries = 1000000
	opts.IndexCacheSize = 0       // Disable index cache for memory savings
	opts.BlockCacheSize = 8 << 20 // 8MB block cache

	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	return &CommentsDB{
		db:   db,
		path: dbPath,
	}, nil
}

// Close closes the database
func (cdb *CommentsDB) Close() error {
	return cdb.db.Close()
}

// Key building helpers

func (cdb *CommentsDB) castKey(hash []byte) []byte {
	key := make([]byte, 0, 5+len(hash))
	key = append(key, "cast:"...)
	return append(key, hash...)
}

func (cdb *CommentsDB) removeKey(hash []byte) []byte {
	key := make([]byte, 0, 7+len(hash))
	key = append(key, "remove:"...)
	return append(key, hash...)
}

func urlKey(url string) []byte {
	key := make([]byte, 0, 4+len(url))
	key = append(key, "url:"...)
	return append(key, url...)
}

func (cdb *CommentsDB) urlIndexKey(url string, blockNumber uint64, eventIdx uint64, hash []byte) []byte {
	// url_idx:<url>:<block_be>:<event_idx_be>:<hash>
	key := make([]byte, 0, 9+len(url)+8+1+8+1+len(hash))
	key = append(key, "url_idx:"...)
	key = append(key, url...)
	key = append(key, ':')

	// Inline binary encoding to avoid allocation
	blockBE := [8]byte{}
	binary.BigEndian.PutUint64(blockBE[:], blockNumber)
	key = append(key, blockBE[:]...)
	key = append(key, ':')

	eventIdxBE := [8]byte{}
	binary.BigEndian.PutUint64(eventIdxBE[:], eventIdx)
	key = append(key, eventIdxBE[:]...)
	key = append(key, ':')

	return append(key, hash...)
}

func urlIndexPrefix(url string) []byte {
	key := make([]byte, 0, 9+len(url))
	key = append(key, "url_idx:"...)
	key = append(key, url...)
	return append(key, ':')
}

func (cdb *CommentsDB) parentIndexKey(parentHash []byte, blockNumber uint64, eventIdx uint64, hash []byte) []byte {
	// parent_idx:<parent_hash>:<block_be>:<event_idx_be>:<hash>
	key := make([]byte, 0, 11+len(parentHash)+8+1+8+1+len(hash))
	key = append(key, "parent_idx:"...)
	key = append(key, parentHash...)
	key = append(key, ':')

	// Inline binary encoding to avoid allocation
	blockBE := [8]byte{}
	binary.BigEndian.PutUint64(blockBE[:], blockNumber)
	key = append(key, blockBE[:]...)
	key = append(key, ':')

	eventIdxBE := [8]byte{}
	binary.BigEndian.PutUint64(eventIdxBE[:], eventIdx)
	key = append(key, eventIdxBE[:]...)
	key = append(key, ':')

	return append(key, hash...)
}

func parentIndexPrefix(parentHash []byte) []byte {
	key := make([]byte, 0, 12+len(parentHash))
	key = append(key, "parent_idx:"...)
	key = append(key, parentHash...)
	return append(key, ':')
}

// Pre-allocate byte array for shard state keys
var shardStateKeyPrefix = []byte("state:shard:")

const userCacheKeyPrefix = "user:"

func shardStateKey(shardID uint32) []byte {
	// Use strings.Builder for efficient string building
	var b strings.Builder
	b.Grow(len(shardStateKeyPrefix) + 10) // Pre-allocate
	b.Write(shardStateKeyPrefix)
	b.WriteString(fmt.Sprintf("%d", shardID))
	return []byte(b.String())
}

func userKey(fid uint64) []byte {
	return []byte(fmt.Sprintf("%s%d", userCacheKeyPrefix, fid))
}

type CachedUserData struct {
	Fid         uint64 `json:"fid"`
	Username    string `json:"username,omitempty"`
	DisplayName string `json:"displayName,omitempty"`
	Avatar      string `json:"avatar,omitempty"`
	UpdatedAt   int64  `json:"updatedAt"`
}

// Database operations

// StoreCast stores a cast message and creates appropriate indexes
func (cdb *CommentsDB) StoreCast(msg *pb.Message, blockNumber uint64, eventIdx uint64, parentUrl string, isRootCast bool) error {
	return cdb.db.Update(func(txn *badger.Txn) error {
		// Serialize message to protobuf
		msgBytes, err := proto.Marshal(msg)
		if err != nil {
			return fmt.Errorf("failed to marshal message: %w", err)
		}

		// Store the cast
		castKeyBytes := cdb.castKey(msg.Hash)
		if err := txn.Set(castKeyBytes, msgBytes); err != nil {
			return err
		}

		// If this is a root cast (parent_url starts with target URL), mark URL as tracked
		if isRootCast {
			urlKeyBytes := urlKey(parentUrl)
			if err := txn.Set(urlKeyBytes, nil); err != nil {
				return err
			}
			// Create URL index
			urlIdxKeyBytes := cdb.urlIndexKey(parentUrl, blockNumber, eventIdx, msg.Hash)
			if err := txn.Set(urlIdxKeyBytes, nil); err != nil {
				return err
			}
		}

		// Create parent index if this cast has a parent cast
		castBody := msg.Data.GetCastAddBody()
		if castBody != nil {
			parentCastId := castBody.GetParentCastId()
			if parentCastId != nil {
				parentIdxKeyBytes := cdb.parentIndexKey(parentCastId.Hash, blockNumber, eventIdx, msg.Hash)
				if err := txn.Set(parentIdxKeyBytes, nil); err != nil {
					return err
				}
			}
		}

		return nil
	})
}

// StoreRemove stores a cast remove message
func (cdb *CommentsDB) StoreRemove(msg *pb.Message) error {
	return cdb.db.Update(func(txn *badger.Txn) error {
		msgBytes, err := proto.Marshal(msg)
		if err != nil {
			return fmt.Errorf("failed to marshal message: %w", err)
		}

		removeBody := msg.Data.GetCastRemoveBody()
		if removeBody == nil {
			return fmt.Errorf("invalid cast remove message")
		}

		removeKeyBytes := cdb.removeKey(removeBody.TargetHash)
		err = txn.Set(removeKeyBytes, msgBytes)
		return err
	})
}

// CastExists checks if a cast exists in the database
func (cdb *CommentsDB) CastExists(hash []byte) (bool, error) {
	var exists bool
	err := cdb.db.View(func(txn *badger.Txn) error {
		castKeyBytes := cdb.castKey(hash)
		_, err := txn.Get(castKeyBytes)
		if err == badger.ErrKeyNotFound {
			exists = false
			return nil
		}
		if err != nil {
			return err
		}
		exists = true
		return nil
	})
	return exists, err
}

// URLTracked checks if a URL is being tracked
func (cdb *CommentsDB) URLTracked(url string) (bool, error) {
	var tracked bool
	err := cdb.db.View(func(txn *badger.Txn) error {
		urlKeyBytes := urlKey(url)
		_, err := txn.Get(urlKeyBytes)
		if err == badger.ErrKeyNotFound {
			tracked = false
			return nil
		}
		if err != nil {
			return err
		}
		tracked = true
		return nil
	})
	return tracked, err
}

// GetShardState retrieves the last processed block number for a shard
func (cdb *CommentsDB) GetShardState(shardID uint32) (uint64, error) {
	var blockNumber uint64
	err := cdb.db.View(func(txn *badger.Txn) error {
		shardKeyBytes := shardStateKey(shardID)
		item, err := txn.Get(shardKeyBytes)
		if err == badger.ErrKeyNotFound {
			blockNumber = 0
			return nil
		}
		if err != nil {
			return err
		}

		return item.Value(func(val []byte) error {
			if len(val) != 8 {
				return fmt.Errorf("invalid block number length: %d", len(val))
			}
			blockNumber = binary.BigEndian.Uint64(val)
			return nil
		})
	})
	return blockNumber, err
}

// SetShardState sets the last processed block number for a shard
func (cdb *CommentsDB) SetShardState(shardID uint32, blockNumber uint64) error {
	return cdb.db.Update(func(txn *badger.Txn) error {
		blockBE := [8]byte{}
		binary.BigEndian.PutUint64(blockBE[:], blockNumber)
		shardKeyBytes := shardStateKey(shardID)
		return txn.Set(shardKeyBytes, blockBE[:])
	})
}

// GetCast retrieves a cast message by hash
func (cdb *CommentsDB) GetCast(hash []byte) (*pb.Message, error) {
	var msg *pb.Message
	err := cdb.db.View(func(txn *badger.Txn) error {
		castKeyBytes := cdb.castKey(hash)
		item, err := txn.Get(castKeyBytes)
		if err != nil {
			return err
		}

		return item.Value(func(val []byte) error {
			msg = &pb.Message{}
			return proto.Unmarshal(val, msg)
		})
	})
	return msg, err
}

// IsRemoved checks if a cast has been removed
func (cdb *CommentsDB) IsRemoved(hash []byte) (bool, error) {
	var removed bool
	err := cdb.db.View(func(txn *badger.Txn) error {
		removeKeyBytes := cdb.removeKey(hash)
		_, err := txn.Get(removeKeyBytes)
		if err == badger.ErrKeyNotFound {
			removed = false
			return nil
		}
		if err != nil {
			return err
		}
		removed = true
		return nil
	})
	return removed, err
}

// GetTrackedURLs returns all tracked URLs that start with the given prefix
func (cdb *CommentsDB) GetTrackedURLs(urlPrefix string) ([]string, error) {
	var urls []string
	prefix := make([]byte, 0, 4+len(urlPrefix))
	prefix = append(prefix, "url:"...)
	prefix = append(prefix, urlPrefix...)

	err := cdb.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false // We only need keys
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			key := item.Key()
			// Extract URL from key (remove "url:" prefix)
			// Avoid string allocation by using unsafe conversion where possible
			url := string(key[4:])
			urls = append(urls, url)
		}
		return nil
	})

	return urls, err
}

// GetRootCastsForURL returns all root cast hashes for a URL
func (cdb *CommentsDB) GetRootCastsForURL(url string) ([][]byte, error) {
	var hashes [][]byte
	prefix := urlIndexPrefix(url)
	hashOffset := len(prefix) + 8 + 1 + 8 + 1 // prefix + block (8) + ':' + event (8) + ':'

	err := cdb.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			key := item.Key()
			if len(key) <= hashOffset {
				continue
			}
			component := key[hashOffset:]
			hashBytes := make([]byte, len(component))
			copy(hashBytes, component)
			hashes = append(hashes, hashBytes)
		}
		return nil
	})

	return hashes, err
}

// GetRepliesForCast returns all reply hashes for a given cast
func (cdb *CommentsDB) GetRepliesForCast(parentHash []byte) ([][]byte, error) {
	var hashes [][]byte
	prefix := parentIndexPrefix(parentHash)
	hashOffset := len(prefix) + 8 + 1 + 8 + 1 // prefix + block (8) + ':' + event (8) + ':'

	err := cdb.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			key := item.Key()
			if len(key) <= hashOffset {
				continue
			}
			component := key[hashOffset:]
			hashBytes := make([]byte, len(component))
			copy(hashBytes, component)
			hashes = append(hashes, hashBytes)
		}
		return nil
	})

	return hashes, err
}

// GetCachedUser retrieves cached user metadata and indicates if it is still fresh
func (cdb *CommentsDB) GetCachedUser(fid uint64, ttl time.Duration) (*CachedUserData, bool, error) {
	var cached *CachedUserData

	err := cdb.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(userKey(fid))
		if err == badger.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}

		return item.Value(func(val []byte) error {
			var data CachedUserData
			if err := json.Unmarshal(val, &data); err != nil {
				return err
			}
			cached = &data
			return nil
		})
	})
	if err != nil {
		return nil, false, err
	}
	if cached == nil {
		return nil, false, nil
	}

	if ttl <= 0 {
		return cached, true, nil
	}

	updatedAt := time.Unix(cached.UpdatedAt, 0)
	if time.Since(updatedAt) > ttl {
		return cached, false, nil
	}
	return cached, true, nil
}

// StoreCachedUser saves user metadata and updates its timestamp
func (cdb *CommentsDB) StoreCachedUser(data *CachedUserData) error {
	if data == nil {
		return fmt.Errorf("nil user data")
	}
	data.UpdatedAt = time.Now().Unix()
	payload, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal cached user data: %w", err)
	}

	return cdb.db.Update(func(txn *badger.Txn) error {
		return txn.Set(userKey(data.Fid), payload)
	})
}
