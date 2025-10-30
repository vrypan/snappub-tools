package commands

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	pb "github.com/vrypan/farcaster-go/farcaster"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	syncStartBlock int64
)

var commentsCmd = &cobra.Command{
	Use:   "comments",
	Short: "Manage comment threads for a URL",
	Long:  `Sync and export comment threads from Farcaster for a specific URL prefix.`,
}

var syncCmd = &cobra.Command{
	Use:   "sync <url>",
	Short: "Sync comments from snapchain blocks",
	Long: `Fetch all castAdd and castRemove messages from snapchain blocks and store
them in the local database for the specified URL prefix.`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		url := args[0]
		return runSync(url)
	},
}

var exportCmd = &cobra.Command{
	Use:   "export <url>",
	Short: "Export synced comments to JSON",
	Long: `Export the synced comment threads to JSON files, one file per tracked URL.
The output will be sorted chronologically for easy reading.`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		url := args[0]
		return runExport(url)
	},
}

func init() {
	rootCmd.AddCommand(commentsCmd)
	commentsCmd.AddCommand(syncCmd)
	commentsCmd.AddCommand(exportCmd)

	// Sync command flags
	syncCmd.Flags().Int64Var(&syncStartBlock, "start-block", 0, "Block number to start from (0=resume from saved state, positive=specific block, negative=current-N blocks)")
}

func runSync(url string) error {
	fmt.Printf("Syncing comments for URL: %s\n", url)

	// Open database
	db, err := OpenCommentsDB()
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}
	defer db.Close()

	// Get node from config
	node := viper.GetString("node")
	if node == "" {
		return fmt.Errorf("node not configured. Use 'snappub config set node <ip:port>'")
	}

	// Create gRPC connection
	conn, err := grpc.Dial(node, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("error connecting to hub: %w", err)
	}
	defer conn.Close()

	client := pb.NewHubServiceClient(conn)
	ctx := context.Background()

	// Get hub info to determine current block heights
	info, err := client.GetInfo(ctx, &pb.GetInfoRequest{})
	if err != nil {
		return fmt.Errorf("failed to get hub info: %w", err)
	}

	// Calculate start blocks for each shard before processing
	// Shard 0 is not actively used, only process shards 1 and 2
	shards := []uint32{1, 2}
	shardStartBlocks := make(map[uint32]uint64)

	for _, shardID := range shards {
		// Find shard info
		var currentBlock uint64
		for _, shardInfo := range info.ShardInfos {
			if shardInfo.ShardId == shardID {
				currentBlock = shardInfo.MaxHeight
				break
			}
		}

		if currentBlock == 0 {
			return fmt.Errorf("shard %d not found in hub info", shardID)
		}

		// Get last processed block for this shard
		lastBlock, err := db.GetShardState(shardID)
		if err != nil {
			return fmt.Errorf("failed to get shard state: %w", err)
		}

		var startBlock uint64

		if syncStartBlock == 0 {
			// Default: resume from saved state
			if lastBlock == 0 {
				startBlock = 1
			} else {
				startBlock = lastBlock + 1
			}
		} else if syncStartBlock > 0 {
			// Positive: use specific block number
			startBlock = uint64(syncStartBlock)
		} else {
			// Negative: current block - N
			offset := uint64(-syncStartBlock)
			if offset >= currentBlock {
				startBlock = 1
			} else {
				startBlock = currentBlock - offset
			}
		}

		shardStartBlocks[shardID] = startBlock
	}

	// Spinner characters
	spinChars := []rune{'⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏'}
	checkChar := '✔'
	spinIdx := 0

	// Combined counters across all shards
	totalChunksProcessed := 0
	totalCastsStored := 0
	totalRemovesStored := 0

	// Process shards
	for _, shardID := range shards {
		startBlock := shardStartBlocks[shardID]

		// Get current block height for this shard to use as stop block
		var currentBlock uint64
		for _, shardInfo := range info.ShardInfos {
			if shardInfo.ShardId == shardID {
				currentBlock = shardInfo.MaxHeight
				break
			}
		}

		// Process in batches to avoid gRPC message size limits
		batchSize := uint64(100)
		for batchStart := startBlock; batchStart <= currentBlock; batchStart += batchSize {
			batchEnd := batchStart + batchSize - 1
			if batchEnd > currentBlock {
				batchEnd = currentBlock
			}

			// Call GetShardChunks for this batch
			response, err := client.GetShardChunks(ctx, &pb.ShardChunksRequest{
				ShardId:          shardID,
				StartBlockNumber: batchStart,
				StopBlockNumber:  &batchEnd,
			})
			if err != nil {
				fmt.Printf("\n") // Clear progress line before error
				return fmt.Errorf("failed to get shard chunks for shard %d (blocks %d-%d): %w", shardID, batchStart, batchEnd, err)
			}

			for _, chunk := range response.ShardChunks {
				blockNum := chunk.Header.Height.BlockNumber

				// Process transactions in this chunk
				for _, tx := range chunk.Transactions {
					// Process user messages in transaction
					for msgIdx, msg := range tx.UserMessages {
						// Process based on message type
						switch msg.Data.Type {
						case pb.MessageType_MESSAGE_TYPE_CAST_ADD:
							stored, err := processCastAdd(db, msg, url, blockNum, uint64(msgIdx))
							if err != nil {
								fmt.Printf("\n") // Clear progress line before error
								return fmt.Errorf("failed to process cast add: %w", err)
							}
							if stored {
								totalCastsStored++
							}

						case pb.MessageType_MESSAGE_TYPE_CAST_REMOVE:
							stored, err := processCastRemove(db, msg)
							if err != nil {
								fmt.Printf("\n") // Clear progress line before error
								return fmt.Errorf("failed to process cast remove: %w", err)
							}
							if stored {
								totalRemovesStored++
							}
						}
					}
				}

				// Update shard state after processing chunk
				if err := db.SetShardState(shardID, blockNum); err != nil {
					fmt.Printf("\n") // Clear progress line before error
					return fmt.Errorf("failed to update shard state: %w", err)
				}

				totalChunksProcessed++

				// Update progress line with spinner
				spinIdx = (spinIdx + 1) % len(spinChars)
				fmt.Printf("\r%c [%d Chunks]  Cast Add: %d  Remove: %d",
					spinChars[spinIdx], totalChunksProcessed, totalCastsStored, totalRemovesStored)
			}
		} // End batch loop
	}

	fmt.Printf("\r%c [%d Chunks]  Cast Add: %d  Remove: %d\n",
		checkChar, totalChunksProcessed, totalCastsStored, totalRemovesStored)

	fmt.Printf("\nSync complete!\n")
	return nil
}

func processCastAdd(db *CommentsDB, msg *pb.Message, targetURL string, blockNum uint64, eventIdx uint64) (bool, error) {
	castBody := msg.Data.GetCastAddBody()
	if castBody == nil {
		return false, nil
	}

	parentUrl := castBody.GetParentUrl()
	parentCastId := castBody.GetParentCastId()

	// Check condition 1: parent_url starts with target URL
	if parentUrl != "" && strings.HasPrefix(parentUrl, targetURL) {
		// This is a root cast
		if err := db.StoreCast(msg, blockNum, eventIdx, parentUrl, true); err != nil {
			return false, err
		}
		return true, nil
	}

	// Check condition 2: parent_url is being tracked
	if parentUrl != "" {
		tracked, err := db.URLTracked(parentUrl)
		if err != nil {
			return false, err
		}
		if tracked {
			// Reply to a tracked URL thread
			if err := db.StoreCast(msg, blockNum, eventIdx, parentUrl, false); err != nil {
				return false, err
			}
			return true, nil
		}
	}

	// Check condition 3: parent_cast_id exists in DB
	if parentCastId != nil {
		exists, err := db.CastExists(parentCastId.Hash)
		if err != nil {
			return false, err
		}
		if exists {
			// Reply to a tracked cast
			// Use the parent URL for indexing purposes
			if err := db.StoreCast(msg, blockNum, eventIdx, parentUrl, false); err != nil {
				return false, err
			}
			return true, nil
		}
	}

	return false, nil
}

func processCastRemove(db *CommentsDB, msg *pb.Message) (bool, error) {
	removeBody := msg.Data.GetCastRemoveBody()
	if removeBody == nil {
		return false, nil
	}

	// Check if the cast exists in our database
	exists, err := db.CastExists(removeBody.TargetHash)
	if err != nil {
		return false, err
	}

	if exists {
		if err := db.StoreRemove(msg); err != nil {
			return false, err
		}
		return true, nil
	}

	return false, nil
}

// JSON export structures
type CastExport struct {
	Hash      string        `json:"hash"`
	FID       uint64        `json:"fid"`
	Timestamp string        `json:"timestamp"`
	Text      string        `json:"text"`
	Embeds    []EmbedExport `json:"embeds,omitempty"`
	Mentions  []uint64      `json:"mentions,omitempty"`
	Removed   bool          `json:"removed"`
	Replies   []CastExport  `json:"replies,omitempty"`
}

type EmbedExport struct {
	URL    string        `json:"url,omitempty"`
	CastID *CastIDExport `json:"castId,omitempty"`
}

type CastIDExport struct {
	FID  uint64 `json:"fid"`
	Hash string `json:"hash"`
}

type ThreadExport struct {
	ParentURL   string       `json:"parentUrl"`
	LastUpdated string       `json:"lastUpdated"`
	RootCasts   []CastExport `json:"rootCasts"`
}

func runExport(url string) error {
	fmt.Printf("Exporting comments for URL: %s\n", url)

	// Open database
	db, err := OpenCommentsDB()
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}
	defer db.Close()

	// Get all tracked URLs that match the prefix
	trackedURLs, err := db.GetTrackedURLs(url)
	if err != nil {
		return fmt.Errorf("failed to get tracked URLs: %w", err)
	}

	if len(trackedURLs) == 0 {
		fmt.Printf("No tracked URLs found matching: %s\n", url)
		return nil
	}

	fmt.Printf("Found %d tracked URL(s)\n", len(trackedURLs))

	// Export each URL
	for _, trackedURL := range trackedURLs {
		fmt.Printf("\nExporting: %s\n", trackedURL)

		// Get root casts for this URL
		rootHashes, err := db.GetRootCastsForURL(trackedURL)
		if err != nil {
			return fmt.Errorf("failed to get root casts: %w", err)
		}

		fmt.Printf("  Found %d root cast(s)\n", len(rootHashes))

		// Build cast tree for each root
		var rootCasts []CastExport
		for _, hash := range rootHashes {
			cast, err := buildCastTree(db, hash)
			if err != nil {
				return fmt.Errorf("failed to build cast tree: %w", err)
			}
			rootCasts = append(rootCasts, *cast)
		}

		// Sort by timestamp
		sort.Slice(rootCasts, func(i, j int) bool {
			return rootCasts[i].Timestamp < rootCasts[j].Timestamp
		})

		// Create export structure
		export := ThreadExport{
			ParentURL:   trackedURL,
			LastUpdated: time.Now().UTC().Format(time.RFC3339),
			RootCasts:   rootCasts,
		}

		// Write JSON file
		filename := sanitizeFilename(trackedURL) + ".json"
		if err := writeJSONFile(filename, export); err != nil {
			return fmt.Errorf("failed to write JSON file: %w", err)
		}

		fmt.Printf("  Exported to: %s\n", filename)
	}

	fmt.Printf("\nExport complete!\n")
	return nil
}

func buildCastTree(db *CommentsDB, hash []byte) (*CastExport, error) {
	// Get the cast message
	msg, err := db.GetCast(hash)
	if err != nil {
		return nil, fmt.Errorf("failed to get cast: %w", err)
	}

	// Check if removed
	removed, err := db.IsRemoved(hash)
	if err != nil {
		return nil, fmt.Errorf("failed to check if removed: %w", err)
	}

	// Extract cast data
	castBody := msg.Data.GetCastAddBody()
	if castBody == nil {
		return nil, fmt.Errorf("invalid cast message")
	}

	// Convert timestamp to ISO 8601
	timestamp := int64(msg.Data.Timestamp) + FARCASTER_EPOCH
	isoTime := time.Unix(timestamp, 0).UTC().Format(time.RFC3339)

	// Build embeds
	var embeds []EmbedExport
	for _, embed := range castBody.Embeds {
		embedExport := EmbedExport{}
		if embed.GetUrl() != "" {
			embedExport.URL = embed.GetUrl()
		} else if embed.GetCastId() != nil {
			castId := embed.GetCastId()
			embedExport.CastID = &CastIDExport{
				FID:  castId.Fid,
				Hash: hex.EncodeToString(castId.Hash),
			}
		}
		embeds = append(embeds, embedExport)
	}

	cast := &CastExport{
		Hash:      hex.EncodeToString(hash),
		FID:       msg.Data.Fid,
		Timestamp: isoTime,
		Text:      castBody.Text,
		Embeds:    embeds,
		Mentions:  castBody.Mentions,
		Removed:   removed,
	}

	// Recursively get replies
	replyHashes, err := db.GetRepliesForCast(hash)
	if err != nil {
		return nil, fmt.Errorf("failed to get replies: %w", err)
	}

	for _, replyHash := range replyHashes {
		reply, err := buildCastTree(db, replyHash)
		if err != nil {
			return nil, fmt.Errorf("failed to build reply tree: %w", err)
		}
		cast.Replies = append(cast.Replies, *reply)
	}

	// Sort replies by timestamp
	sort.Slice(cast.Replies, func(i, j int) bool {
		return cast.Replies[i].Timestamp < cast.Replies[j].Timestamp
	})

	return cast, nil
}

func sanitizeFilename(url string) string {
	// Remove protocol
	filename := strings.TrimPrefix(url, "https://")
	filename = strings.TrimPrefix(filename, "http://")

	// Replace invalid characters with underscore
	re := regexp.MustCompile(`[^\w\-.]`)
	filename = re.ReplaceAllString(filename, "_")

	// Limit length
	if len(filename) > 200 {
		filename = filename[:200]
	}

	return filename
}

func writeJSONFile(filename string, data interface{}) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	return encoder.Encode(data)
}
