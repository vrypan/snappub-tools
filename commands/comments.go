package commands

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
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
	exportOutDir   string
)

const userCacheTTL = 6 * time.Hour

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
	exportCmd.Flags().StringVarP(&exportOutDir, "out", "o", ".", "Directory to write export JSON files")
}

func fetchAndCacheUserData(ctx context.Context, client pb.HubServiceClient, db *CommentsDB, fid uint64) (*CachedUserData, error) {
	if client == nil {
		return nil, fmt.Errorf("no hub client available")
	}

	reqCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	resp, err := client.GetUserDataByFid(reqCtx, &pb.FidRequest{Fid: fid})
	if err != nil {
		return nil, err
	}

	profile := &CachedUserData{Fid: fid}
	for _, msg := range resp.Messages {
		body := msg.Data.GetUserDataBody()
		if body == nil {
			continue
		}
		switch body.Type {
		case pb.UserDataType_USER_DATA_TYPE_USERNAME:
			profile.Username = body.Value
		case pb.UserDataType_USER_DATA_TYPE_DISPLAY:
			profile.DisplayName = body.Value
		case pb.UserDataType_USER_DATA_TYPE_PFP:
			profile.Avatar = body.Value
		}
	}

	if err := db.StoreCachedUser(profile); err != nil {
		return nil, err
	}
	return profile, nil
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
	conn, err := grpc.NewClient(node, grpc.WithTransportCredentials(insecure.NewCredentials()))
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

	seenFids := make(map[uint64]struct{})
	recordFID := func(fid uint64) {
		if fid == 0 {
			return
		}
		seenFids[fid] = struct{}{}
	}

	// Create shard info map for faster lookups
	shardInfoMap := make(map[uint32]*pb.ShardInfo)
	for _, shardInfo := range info.ShardInfos {
		shardInfoMap[shardInfo.ShardId] = shardInfo
	}

	// Calculate start blocks for each shard before processing
	// Shard 0 is not actively used, only process shards 1 and 2
	shards := []uint32{1, 2}
	shardStartBlocks := make(map[uint32]uint64)

	for _, shardID := range shards {
		// Find shard info
		shardInfo, ok := shardInfoMap[shardID]
		if !ok {
			return fmt.Errorf("shard %d not found in hub info", shardID)
		}
		currentBlock := shardInfo.MaxHeight

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

	// Spinner characters (clockwise)
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
		currentBlock := shardInfoMap[shardID].MaxHeight

		// Process in batches to avoid gRPC message size limits
		const batchSize = uint64(100)
		for batchStart := startBlock; batchStart <= currentBlock; batchStart += batchSize {
			batchEnd := min(batchStart+batchSize-1, currentBlock)

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
							castBody := msg.Data.GetCastAddBody()
							stored, err := processCastAdd(db, msg, url, blockNum, uint64(msgIdx))
							if err != nil {
								fmt.Printf("\n") // Clear progress line before error
								return fmt.Errorf("failed to process cast add: %w", err)
							}
							if stored {
								recordFID(msg.Data.Fid)
								if castBody != nil {
									for _, fid := range castBody.Mentions {
										recordFID(fid)
									}
									if parentCastId := castBody.GetParentCastId(); parentCastId != nil {
										recordFID(parentCastId.Fid)
									}
									for _, embed := range castBody.Embeds {
										if embed.GetCastId() != nil {
											recordFID(embed.GetCastId().Fid)
										}
									}
								}
								totalCastsStored++
							}

						case pb.MessageType_MESSAGE_TYPE_CAST_REMOVE:
							stored, err := processCastRemove(db, msg)
							if err != nil {
								fmt.Printf("\n") // Clear progress line before error
								return fmt.Errorf("failed to process cast remove: %w", err)
							}
							if stored {
								recordFID(msg.Data.Fid)
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
				fmt.Printf("\r%c Parsing blocks [%d Chunks]  Cast Add: %d  Remove: %d",
					spinChars[spinIdx], totalChunksProcessed, totalCastsStored, totalRemovesStored)
			}
		} // End batch loop
	}

	fmt.Printf("\r%c Parsing blocks [%d Chunks]  Cast Add: %d  Remove: %d\n",
		checkChar, totalChunksProcessed, totalCastsStored, totalRemovesStored)

	fidList := make([]uint64, 0, len(seenFids))
	for fid := range seenFids {
		fidList = append(fidList, fid)
	}
	slices.Sort(fidList)

	if len(fidList) == 0 {
		fmt.Printf("%c Parsing user data: no fids referenced\n", checkChar)
	} else {
		totalFids := len(fidList)
		processedUsers := 0
		userSpinIdx := 0
		fmt.Printf("User data %c [%d/%d]", spinChars[userSpinIdx], processedUsers, totalFids)
		for _, fid := range fidList {
			cached, fresh, err := db.GetCachedUser(fid, userCacheTTL)
			if err != nil {
				fmt.Printf("\nWarning: failed to read cached user data for fid %d: %v\n", fid, err)
				fresh = false
			}
			if cached == nil {
				if err := db.StoreUserPlaceholder(fid); err != nil {
					fmt.Printf("\nWarning: failed to store user placeholder for fid %d: %v\n", fid, err)
				}
				fresh = false
			}
			if !fresh {
				if _, err := fetchAndCacheUserData(ctx, client, db, fid); err != nil {
					fmt.Printf("\nWarning: failed to fetch user data for fid %d: %v\n", fid, err)
				}
			}
			processedUsers++
			userSpinIdx = (userSpinIdx + 1) % len(spinChars)
			fmt.Printf("\r%c Parsing user data [%d/%d]", spinChars[userSpinIdx], processedUsers, totalFids)
		}
		fmt.Printf("\r%c Parsing user data [%d/%d]\n", checkChar, totalFids, totalFids)
	}

	fmt.Printf("\nSync complete!\n")
	return nil
}

func processCastAdd(db *CommentsDB, msg *pb.Message, targetURL string, blockNum uint64, eventIdx uint64) (bool, error) {
	castBody := msg.Data.GetCastAddBody()
	if castBody == nil {
		return false, nil
	}

	// Skip if we already have this cast stored
	exists, err := db.CastExists(msg.Hash)
	if err != nil {
		return false, err
	}
	if exists {
		return true, nil
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

type UserExport struct {
	Username    string `json:"username,omitempty"`
	DisplayName string `json:"displayName,omitempty"`
	Avatar      string `json:"avatar,omitempty"`
}

type ThreadExport struct {
	ParentURL   string                `json:"parentUrl"`
	LastUpdated string                `json:"lastUpdated"`
	RootCasts   []CastExport          `json:"rootCasts"`
	Users       map[string]UserExport `json:"users"`
}

type UserCollector struct {
	ctx    context.Context
	db     *CommentsDB
	client pb.HubServiceClient
	ttl    time.Duration
	fids   map[uint64]struct{}
}

func NewUserCollector(ctx context.Context, db *CommentsDB, client pb.HubServiceClient, ttl time.Duration) *UserCollector {
	return &UserCollector{
		ctx:    ctx,
		db:     db,
		client: client,
		ttl:    ttl,
		fids:   make(map[uint64]struct{}),
	}
}

func (uc *UserCollector) RecordFID(fid uint64) {
	if fid == 0 {
		return
	}
	uc.fids[fid] = struct{}{}
}

func (uc *UserCollector) RecordMentions(fids []uint64) {
	for _, fid := range fids {
		uc.RecordFID(fid)
	}
}

func (uc *UserCollector) BuildUserMap() (map[string]UserExport, error) {
	users := make(map[string]UserExport)
	for fid := range uc.fids {
		cached, fresh, err := uc.db.GetCachedUser(fid, uc.ttl)
		if err != nil {
			return nil, fmt.Errorf("failed to read cached user data for fid %d: %w", fid, err)
		}

		if !fresh && uc.client != nil {
			profile, err := fetchAndCacheUserData(uc.ctx, uc.client, uc.db, fid)
			if err != nil {
				fmt.Printf("\nWarning: failed to refresh user data for fid %d: %v\n", fid, err)
			} else {
				cached = profile
				fresh = true
			}
		}

		if cached != nil {
			users[strconv.FormatUint(fid, 10)] = UserExport{
				Username:    cached.Username,
				DisplayName: cached.DisplayName,
				Avatar:      cached.Avatar,
			}
		}
	}
	return users, nil
}

func runExport(url string) error {
	fmt.Printf("Exporting comments for URL: %s\n", url)

	// Open database
	db, err := OpenCommentsDB()
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}
	defer db.Close()

	ctx := context.Background()

	var userConn *grpc.ClientConn
	var userClient pb.HubServiceClient
	node := viper.GetString("node")
	if node != "" {
		conn, err := grpc.NewClient(node, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			fmt.Printf("Warning: failed to connect to hub for user data lookups: %v\n", err)
		} else {
			userConn = conn
			userClient = pb.NewHubServiceClient(conn)
		}
	}
	if userConn != nil {
		defer userConn.Close()
	}

	outDir := exportOutDir
	if outDir == "" {
		outDir = "."
	}
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}

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

		collector := NewUserCollector(ctx, db, userClient, userCacheTTL)

		// Get root casts for this URL
		rootHashes, err := db.GetRootCastsForURL(trackedURL)
		if err != nil {
			return fmt.Errorf("failed to get root casts: %w", err)
		}

		fmt.Printf("  Found %d root cast(s)\n", len(rootHashes))

		// Build cast tree for each root
		var rootCasts []CastExport
		for _, hash := range rootHashes {
			cast, err := buildCastTree(db, hash, collector)
			if err != nil {
				return fmt.Errorf("failed to build cast tree: %w", err)
			}
			rootCasts = append(rootCasts, *cast)
		}

		// Sort by timestamp
		slices.SortFunc(rootCasts, func(a, b CastExport) int {
			if a.Timestamp < b.Timestamp {
				return -1
			}
			if a.Timestamp > b.Timestamp {
				return 1
			}
			return 0
		})

		// Create export structure
		usersMap, err := collector.BuildUserMap()
		if err != nil {
			return fmt.Errorf("failed to build user map: %w", err)
		}

		export := ThreadExport{
			ParentURL:   trackedURL,
			LastUpdated: time.Now().UTC().Format(time.RFC3339),
			RootCasts:   rootCasts,
			Users:       usersMap,
		}

		// Write JSON file
		filename := sanitizeFilename(trackedURL) + ".json"
		outputPath := filepath.Join(outDir, filename)
		if err := writeJSONFile(outputPath, export); err != nil {
			return fmt.Errorf("failed to write JSON file: %w", err)
		}

		fmt.Printf("  Exported to: %s\n", outputPath)
	}

	fmt.Printf("\nExport complete!\n")
	return nil
}

func buildCastTree(db *CommentsDB, hash []byte, collector *UserCollector) (*CastExport, error) {
	// Get the cast message
	msg, err := db.GetCast(hash)
	if err != nil {
		return nil, fmt.Errorf("failed to get cast: %w", err)
	}

	if collector != nil {
		collector.RecordFID(msg.Data.Fid)
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
	embeds := make([]EmbedExport, 0, len(castBody.Embeds))
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
			if collector != nil {
				collector.RecordFID(castId.Fid)
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

	if collector != nil {
		collector.RecordMentions(castBody.Mentions)
		if parentCastId := castBody.GetParentCastId(); parentCastId != nil {
			collector.RecordFID(parentCastId.Fid)
		}
	}

	// Recursively get replies
	replyHashes, err := db.GetRepliesForCast(hash)
	if err != nil {
		return nil, fmt.Errorf("failed to get replies: %w", err)
	}

	cast.Replies = make([]CastExport, 0, len(replyHashes))
	for _, replyHash := range replyHashes {
		reply, err := buildCastTree(db, replyHash, collector)
		if err != nil {
			return nil, fmt.Errorf("failed to build reply tree: %w", err)
		}
		cast.Replies = append(cast.Replies, *reply)
	}

	// Sort replies by timestamp
	slices.SortFunc(cast.Replies, func(a, b CastExport) int {
		if a.Timestamp < b.Timestamp {
			return -1
		}
		if a.Timestamp > b.Timestamp {
			return 1
		}
		return 0
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

func writeJSONFile(filename string, data any) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	return encoder.Encode(data)
}
