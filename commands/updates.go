package commands

import (
	"context"
	"fmt"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	pb "github.com/vrypan/farcaster-go/farcaster"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	updatesFname   string
	updatesFid     uint64
	updatesResults uint32
	updatesSince   string
)

var updatesCmd = &cobra.Command{
	Use:   "updates [url]",
	Short: "List casts with parentUrl",
	Long: `List casts by FID that have a parentUrl set.

Either --fid or --fname must be provided (but not both).
If --fname is provided, the FID will be looked up via GetUsernameProof.

If [url] is provided, only casts with that specific parentUrl will be shown.
If [url] is not provided, all casts with any parentUrl will be shown.

Use --results to limit the number of results (default: 10).
Use --since to filter casts after a specific timestamp (ISO 8601 format).`,
	Args: cobra.MaximumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		var filterUrl string
		if len(args) > 0 {
			filterUrl = args[0]
		}

		// One of --fid or --fname MUST be provided
		if updatesFid == 0 && updatesFname == "" {
			return fmt.Errorf("must specify either --fid or --fname")
		}

		if updatesFid != 0 && updatesFname != "" {
			return fmt.Errorf("cannot specify both --fid and --fname")
		}

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

		// If fname is provided, look up the FID
		if updatesFname != "" {
			usernameProof, err := client.GetUsernameProof(ctx, &pb.UsernameProofRequest{
				Name: []byte(updatesFname),
			})
			if err != nil {
				return fmt.Errorf("error looking up fname '%s': %w", updatesFname, err)
			}
			updatesFid = usernameProof.Fid
		}

		// Build the request
		reverse := true
		request := &pb.FidTimestampRequest{
			Fid:     updatesFid,
			Reverse: &reverse,
		}

		// Parse --since timestamp if provided and add to request
		if updatesSince != "" {
			sinceTime, err := time.Parse(time.RFC3339, updatesSince)
			if err != nil {
				return fmt.Errorf("invalid --since timestamp (use ISO 8601 format): %w", err)
			}
			// Convert to Farcaster timestamp (subtract epoch)
			// Add 1 to make it exclusive (> instead of >=)
			farcasterTimestamp := uint64(sinceTime.Unix() - FARCASTER_EPOCH)
			request.StartTimestamp = &farcasterTimestamp
		}

		// Get all cast messages by FID
		response, err := client.GetAllCastMessagesByFid(ctx, request)
		if err != nil {
			return fmt.Errorf("error fetching casts: %w", err)
		}

		// Parse --since timestamp for client-side filtering if needed
		var sinceTimestamp int64
		if updatesSince != "" {
			sinceTime, err := time.Parse(time.RFC3339, updatesSince)
			if err != nil {
				return fmt.Errorf("invalid --since timestamp (use ISO 8601 format): %w", err)
			}
			sinceTimestamp = sinceTime.Unix()
		}

		// Filter and display results
		count := uint32(0)
		for _, msg := range response.Messages {
			if count >= updatesResults {
				break
			}

			// Only process CastAdd messages
			if msg.Data.Type != pb.MessageType_MESSAGE_TYPE_CAST_ADD {
				continue
			}

			castBody := msg.Data.GetCastAddBody()
			if castBody == nil {
				continue
			}

			// Check if this cast has a parentUrl
			parentUrl := castBody.GetParentUrl()
			if parentUrl == "" {
				continue
			}

			// Convert timestamp to Unix timestamp
			timestamp := int64(msg.Data.Timestamp) + FARCASTER_EPOCH

			// If --since is specified, filter by timestamp (exclusive)
			if updatesSince != "" && timestamp <= sinceTimestamp {
				continue
			}

			// If filterUrl is specified, only show matching casts
			if filterUrl != "" && parentUrl != filterUrl {
				continue
			}

			// Convert timestamp to ISO 8601
			isoTime := time.Unix(timestamp, 0).UTC().Format(time.RFC3339)

			// Output: <url> <timestamp>
			fmt.Printf("%s %s\n", parentUrl, isoTime)
			count++
		}

		return nil
	},
}

func init() {
	rootCmd.AddCommand(updatesCmd)
	updatesCmd.Flags().StringVar(&updatesFname, "fname", "", "Farcaster username")
	updatesCmd.Flags().Uint64Var(&updatesFid, "fid", 0, "Farcaster ID (FID)")
	updatesCmd.Flags().Uint32Var(&updatesResults, "results", 10, "Number of results to return")
	updatesCmd.Flags().StringVar(&updatesSince, "since", "", "Show only casts after this timestamp (ISO 8601 format)")
}
