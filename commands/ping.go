package commands

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	pb "github.com/vrypan/farcaster-go/farcaster"
	"github.com/zeebo/blake3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

const FARCASTER_EPOCH int64 = 1609459200

var (
	appKey string
	fname  string
	fid    uint64
	debug  bool
)

var pingCmd = &cobra.Command{
	Use:   "ping <url>",
	Short: "Post a new cast with empty body and parentUrl",
	Long: `Post a new cast with empty body and parentUrl=<url>.

Either --fid or --fname must be provided (but not both).
If --fname is provided, the FID will be looked up via GetUsernameProof.

If --appkey is not provided, the key will be looked up from config using appkeys.<fid>.`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		url := args[0]

		// One of --fid or --fname MUST be provided
		if fid == 0 && fname == "" {
			return fmt.Errorf("must specify either --fid or --fname")
		}

		if fid != 0 && fname != "" {
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
		if fname != "" {
			usernameProof, err := client.GetUsernameProof(ctx, &pb.UsernameProofRequest{
				Name: []byte(fname),
			})
			if err != nil {
				return fmt.Errorf("error looking up fname '%s': %w", fname, err)
			}
			fid = usernameProof.Fid
			fmt.Printf("Resolved %s to FID: %d\n", fname, fid)
		}

		// Determine which key to use
		var key string
		if appKey != "" {
			key = appKey
		} else {
			// Look up key from config using appkeys.<fid>
			appKeys := viper.GetStringMapString("appKeys")
			if appKeys == nil {
				return fmt.Errorf("no appKeys configured")
			}
			fidStr := fmt.Sprintf("%d", fid)
			var ok bool
			key, ok = appKeys[fidStr]
			if !ok {
				return fmt.Errorf("no key found for fid %d in config", fid)
			}
		}

		// Decode private key from hex (remove 0x prefix if present)
		if len(key) > 2 && key[:2] == "0x" {
			key = key[2:]
		}
		privateKeyBytes, err := hex.DecodeString(key)
		if err != nil {
			return fmt.Errorf("error decoding private key: %w", err)
		}

		// Create ed25519 key from seed
		expandedKey := ed25519.NewKeyFromSeed(privateKeyBytes)
		publicKey := expandedKey.Public().(ed25519.PublicKey)

		// Create cast add body with empty text, parent URL and snuppub:update
		castAddBody := &pb.CastAddBody{
			Text: "\n",
			Parent: &pb.CastAddBody_ParentUrl{
				ParentUrl: url,
			},
			Embeds: []*pb.Embed{
				{
					Embed: &pb.Embed_Url{Url: "snuppub:update"},
				},
			},
		}

		// Create message data

		messageData := &pb.MessageData{
			Type:      pb.MessageType_MESSAGE_TYPE_CAST_ADD,
			Fid:       fid,
			Timestamp: uint32(time.Now().Unix() - FARCASTER_EPOCH),
			Network:   pb.FarcasterNetwork_FARCASTER_NETWORK_MAINNET,
			Body: &pb.MessageData_CastAddBody{
				CastAddBody: castAddBody,
			},
		}

		// Marshal message data
		dataBytes, err := proto.Marshal(messageData)
		if err != nil {
			return fmt.Errorf("error marshaling message data: %w", err)
		}

		// Create hash using BLAKE3
		hasher := blake3.New()
		hasher.Write(dataBytes)
		hash := hasher.Sum(nil)[:20]

		// Sign the hash
		signature := ed25519.Sign(expandedKey, hash)

		// Create the complete message
		message := &pb.Message{
			Data:            messageData,
			Hash:            hash,
			HashScheme:      pb.HashScheme_HASH_SCHEME_BLAKE3,
			Signature:       signature,
			SignatureScheme: pb.SignatureScheme_SIGNATURE_SCHEME_ED25519,
			Signer:          publicKey,
			DataBytes:       dataBytes,
		}

		// Print the generated cast hash before submitting
		fmt.Printf("Cast hash: 0x%s\n", hex.EncodeToString(hash))

		// Submit message
		result, err := client.SubmitMessage(ctx, message)
		if err != nil {
			return fmt.Errorf("error submitting cast: %w", err)
		}

		fmt.Printf("Cast submitted successfully!\n")
		fmt.Printf("FID: %d, Hash: 0x%s\n", result.Data.Fid, hex.EncodeToString(result.Hash))

		// Show debug output if requested
		if debug {
			jsonBytes, err := protojson.MarshalOptions{
				Multiline:       true,
				Indent:          "  ",
				EmitUnpopulated: false,
			}.Marshal(result)
			if err != nil {
				return fmt.Errorf("error marshaling response to JSON: %w", err)
			}
			fmt.Printf("\nServer response:\n%s\n", string(jsonBytes))
		}

		return nil
	},
}

func init() {
	rootCmd.AddCommand(pingCmd)
	pingCmd.Flags().StringVar(&appKey, "appkey", "", "Application key (hex)")
	pingCmd.Flags().StringVar(&fname, "fname", "", "Farcaster username (uses key from config)")
	pingCmd.Flags().Uint64Var(&fid, "fid", 0, "Farcaster ID (FID)")
	pingCmd.Flags().BoolVar(&debug, "debug", false, "Show server response as JSON")
}
