package commands

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"

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

func castKey(hash []byte) []byte {
	key := make([]byte, 0, 5+len(hash)*2)
	key = append(key, []byte("cast:")...)
	key = append(key, []byte(hex.EncodeToString(hash))...)
	return key
}

func removeKey(hash []byte) []byte {
	key := make([]byte, 0, 7+len(hash)*2)
	key = append(key, []byte("remove:")...)
	key = append(key, []byte(hex.EncodeToString(hash))...)
	return key
}

func urlKey(url string) []byte {
	return []byte("url:" + url)
}

func urlIndexKey(url string, blockNumber uint64, eventIdx uint64, hash []byte) []byte {
	// url_idx:<url>:<block_be>:<event_idx_be>:<hash>
	blockBE := make([]byte, 8)
	eventIdxBE := make([]byte, 8)
	binary.BigEndian.PutUint64(blockBE, blockNumber)
	binary.BigEndian.PutUint64(eventIdxBE, eventIdx)

	key := make([]byte, 0, 8+len(url)+8+8+len(hash)*2)
	key = append(key, []byte("url_idx:")...)
	key = append(key, []byte(url)...)
	key = append(key, ':')
	key = append(key, blockBE...)
	key = append(key, ':')
	key = append(key, eventIdxBE...)
	key = append(key, ':')
	key = append(key, []byte(hex.EncodeToString(hash))...)
	return key
}

func urlIndexPrefix(url string) []byte {
	return []byte("url_idx:" + url + ":")
}

func parentIndexKey(parentHash []byte, blockNumber uint64, eventIdx uint64, hash []byte) []byte {
	// parent_idx:<parent_hash>:<block_be>:<event_idx_be>:<hash>
	blockBE := make([]byte, 8)
	eventIdxBE := make([]byte, 8)
	binary.BigEndian.PutUint64(blockBE, blockNumber)
	binary.BigEndian.PutUint64(eventIdxBE, eventIdx)

	parentHashHex := hex.EncodeToString(parentHash)
	hashHex := hex.EncodeToString(hash)

	key := make([]byte, 0, 11+len(parentHashHex)+8+8+len(hashHex))
	key = append(key, []byte("parent_idx:")...)
	key = append(key, []byte(parentHashHex)...)
	key = append(key, ':')
	key = append(key, blockBE...)
	key = append(key, ':')
	key = append(key, eventIdxBE...)
	key = append(key, ':')
	key = append(key, []byte(hashHex)...)
	return key
}

func parentIndexPrefix(parentHash []byte) []byte {
	return []byte("parent_idx:" + hex.EncodeToString(parentHash) + ":")
}

func shardStateKey(shardID uint32) []byte {
	return []byte(fmt.Sprintf("state:shard:%d", shardID))
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
		if err := txn.Set(castKey(msg.Hash), msgBytes); err != nil {
			return err
		}

		// If this is a root cast (parent_url starts with target URL), mark URL as tracked
		if isRootCast {
			if err := txn.Set(urlKey(parentUrl), []byte{}); err != nil {
				return err
			}
			// Create URL index
			if err := txn.Set(urlIndexKey(parentUrl, blockNumber, eventIdx, msg.Hash), []byte{}); err != nil {
				return err
			}
		}

		// Create parent index if this cast has a parent cast
		castBody := msg.Data.GetCastAddBody()
		if castBody != nil {
			parentCastId := castBody.GetParentCastId()
			if parentCastId != nil {
				if err := txn.Set(parentIndexKey(parentCastId.Hash, blockNumber, eventIdx, msg.Hash), []byte{}); err != nil {
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

		return txn.Set(removeKey(removeBody.TargetHash), msgBytes)
	})
}

// CastExists checks if a cast exists in the database
func (cdb *CommentsDB) CastExists(hash []byte) (bool, error) {
	var exists bool
	err := cdb.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(castKey(hash))
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
		_, err := txn.Get(urlKey(url))
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
		item, err := txn.Get(shardStateKey(shardID))
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
		blockBE := make([]byte, 8)
		binary.BigEndian.PutUint64(blockBE, blockNumber)
		return txn.Set(shardStateKey(shardID), blockBE)
	})
}

// GetCast retrieves a cast message by hash
func (cdb *CommentsDB) GetCast(hash []byte) (*pb.Message, error) {
	var msg *pb.Message
	err := cdb.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(castKey(hash))
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
		_, err := txn.Get(removeKey(hash))
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
	prefix := []byte("url:" + urlPrefix)

	err := cdb.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false // We only need keys
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			key := item.Key()
			// Extract URL from key (remove "url:" prefix)
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

	err := cdb.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			key := item.Key()
			// Extract hash from key (last component after last ':')
			// Format: url_idx:<url>:<block_be>:<event_idx_be>:<hash>
			// Find the last 3 colons and get everything after the last one
			colonCount := 0
			lastColonIdx := -1
			for i := len(key) - 1; i >= 0; i-- {
				if key[i] == ':' {
					colonCount++
					if colonCount == 1 {
						lastColonIdx = i
						break
					}
				}
			}
			if lastColonIdx >= 0 && lastColonIdx < len(key)-1 {
				hashHex := string(key[lastColonIdx+1:])
				hash, err := hex.DecodeString(hashHex)
				if err != nil {
					return fmt.Errorf("failed to decode hash: %w", err)
				}
				hashes = append(hashes, hash)
			}
		}
		return nil
	})

	return hashes, err
}

// GetRepliesForCast returns all reply hashes for a given cast
func (cdb *CommentsDB) GetRepliesForCast(parentHash []byte) ([][]byte, error) {
	var hashes [][]byte
	prefix := parentIndexPrefix(parentHash)

	err := cdb.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			key := item.Key()
			// Extract hash from key (last component)
			colonCount := 0
			lastColonIdx := -1
			for i := len(key) - 1; i >= 0; i-- {
				if key[i] == ':' {
					colonCount++
					if colonCount == 1 {
						lastColonIdx = i
						break
					}
				}
			}
			if lastColonIdx >= 0 && lastColonIdx < len(key)-1 {
				hashHex := string(key[lastColonIdx+1:])
				hash, err := hex.DecodeString(hashHex)
				if err != nil {
					return fmt.Errorf("failed to decode hash: %w", err)
				}
				hashes = append(hashes, hash)
			}
		}
		return nil
	})

	return hashes, err
}
