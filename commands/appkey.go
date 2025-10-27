package commands

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/btcsuite/btcd/btcutil/hdkeychain"
	"github.com/btcsuite/btcd/chaincfg"
	"github.com/ethereum/go-ethereum/common/math"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/signer/core/apitypes"
	"github.com/skip2/go-qrcode"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/tyler-smith/go-bip39"
)

var appkeyCmd = &cobra.Command{
	Use:   "appkey",
	Short: "Manage application keys",
	Long:  `Generate and manage Farcaster application keys (signers).`,
}

var generateCmd = &cobra.Command{
	Use:   "generate",
	Short: "Generate a new application key",
	Long: `Generate a new ed25519 keypair and request approval from Farcaster.

Requires configuration:
- developer.fid: Your developer/app FID
- developer.mnemonic: Your developer account mnemonic phrase

The command will:
1. Generate a new ed25519 keypair
2. Create a signed key request
3. Display approval URL and QR code
4. Poll for user approval
5. Save the approved key to config`,
	RunE: runGenerate,
}

type SignedKeyRequestResponse struct {
	Result struct {
		SignedKeyRequest struct {
			Token       string `json:"token"`
			DeeplinkUrl string `json:"deeplinkUrl"`
		} `json:"signedKeyRequest"`
	} `json:"result"`
}

type PollResponse struct {
	Result struct {
		SignedKeyRequest struct {
			State   string `json:"state"`
			UserFid int    `json:"userFid"`
		} `json:"signedKeyRequest"`
	} `json:"result"`
}

func runGenerate(cmd *cobra.Command, args []string) error {
	// Get developer FID and mnemonic from config
	devFid := viper.GetInt("developer.fid")
	if devFid == 0 {
		return fmt.Errorf("developer.fid not configured. Use 'snappub config set developer.fid <fid>'")
	}

	devMnemonic := viper.GetString("developer.mnemonic")
	if devMnemonic == "" {
		return fmt.Errorf("developer.mnemonic not configured. Use 'snappub config set developer.mnemonic \"<phrase>\"'")
	}

	// Generate new ed25519 keypair
	fmt.Println("Generating new ed25519 keypair...")
	publicKey, privateKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		return fmt.Errorf("error generating keypair: %w", err)
	}

	publicKeyHex := "0x" + hex.EncodeToString(publicKey)
	privateKeyHex := "0x" + hex.EncodeToString(privateKey.Seed())

	fmt.Printf("Public key: %s\n", publicKeyHex)
	fmt.Printf("Private key: %s\n", privateKeyHex)

	// Derive Ethereum account from mnemonic using BIP-44 path m/44'/60'/0'/0/0
	seed, err := bip39.NewSeedWithErrorChecking(devMnemonic, "")
	if err != nil {
		return fmt.Errorf("error generating seed from mnemonic: %w", err)
	}

	ethPrivateKey, err := derivePrivateKey(seed)
	if err != nil {
		return fmt.Errorf("error deriving private key: %w", err)
	}

	account := crypto.PubkeyToAddress(ethPrivateKey.PublicKey)
	fmt.Printf("Developer address: %s\n", account.Hex())

	// Create EIP-712 signature
	deadline := time.Now().Unix() + 86400 // 24 hours from now

	typedData := apitypes.TypedData{
		Types: apitypes.Types{
			"EIP712Domain": []apitypes.Type{
				{Name: "name", Type: "string"},
				{Name: "version", Type: "string"},
				{Name: "chainId", Type: "uint256"},
				{Name: "verifyingContract", Type: "address"},
			},
			"SignedKeyRequest": []apitypes.Type{
				{Name: "requestFid", Type: "uint256"},
				{Name: "key", Type: "bytes"},
				{Name: "deadline", Type: "uint256"},
			},
		},
		PrimaryType: "SignedKeyRequest",
		Domain: apitypes.TypedDataDomain{
			Name:              "Farcaster SignedKeyRequestValidator",
			Version:           "1",
			ChainId:           math.NewHexOrDecimal256(10),
			VerifyingContract: "0x00000000fc700472606ed4fa22623acf62c60553",
		},
		Message: apitypes.TypedDataMessage{
			"requestFid": math.NewHexOrDecimal256(int64(devFid)),
			"key":        publicKeyHex,
			"deadline":   math.NewHexOrDecimal256(deadline),
		},
	}

	domainSeparator, err := typedData.HashStruct("EIP712Domain", typedData.Domain.Map())
	if err != nil {
		return fmt.Errorf("error hashing domain: %w", err)
	}

	typedDataHash, err := typedData.HashStruct(typedData.PrimaryType, typedData.Message)
	if err != nil {
		return fmt.Errorf("error hashing typed data: %w", err)
	}

	rawData := []byte(fmt.Sprintf("\x19\x01%s%s", string(domainSeparator), string(typedDataHash)))
	signHash := crypto.Keccak256Hash(rawData)

	signature, err := crypto.Sign(signHash.Bytes(), ethPrivateKey)
	if err != nil {
		return fmt.Errorf("error signing: %w", err)
	}

	// Adjust V value for Ethereum signature
	if signature[64] < 27 {
		signature[64] += 27
	}

	signatureHex := "0x" + hex.EncodeToString(signature)

	// POST to Warpcast API
	fmt.Println("\nSubmitting signed key request to Warpcast...")
	requestBody := map[string]interface{}{
		"key":        publicKeyHex,
		"signature":  signatureHex,
		"requestFid": devFid,
		"deadline":   deadline,
	}

	jsonData, err := json.Marshal(requestBody)
	if err != nil {
		return fmt.Errorf("error marshaling request: %w", err)
	}

	resp, err := http.Post(
		"https://api.warpcast.com/v2/signed-key-requests",
		"application/json",
		bytes.NewBuffer(jsonData),
	)
	if err != nil {
		return fmt.Errorf("error posting to Warpcast API: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return fmt.Errorf("Warpcast API returned status %d", resp.StatusCode)
	}

	var signedKeyResp SignedKeyRequestResponse
	if err := json.NewDecoder(resp.Body).Decode(&signedKeyResp); err != nil {
		return fmt.Errorf("error decoding response: %w", err)
	}

	token := signedKeyResp.Result.SignedKeyRequest.Token
	approvalUrl := signedKeyResp.Result.SignedKeyRequest.DeeplinkUrl

	fmt.Printf("\n✓ Signed key request created!\n")
	fmt.Printf("Token: %s\n", token)
	fmt.Printf("Approval URL: %s\n\n", approvalUrl)

	// Generate QR code
	qr, err := qrcode.New(approvalUrl, qrcode.Medium)
	if err != nil {
		return fmt.Errorf("error generating QR code: %w", err)
	}

	// Display QR code in terminal
	fmt.Println("Scan this QR code with your phone to approve:")
	fmt.Println(qr.ToSmallString(false))

	// Poll for approval
	fmt.Println("\nWaiting for approval...")
	userFid, err := pollForApproval(token)
	if err != nil {
		return err
	}

	fmt.Printf("\n✓ Key approved by FID %d!\n", userFid)

	// Save to config
	fmt.Println("Saving key to config...")
	viper.Set(fmt.Sprintf("appKeys.%d", userFid), privateKeyHex)

	home, err := os.UserHomeDir()
	if err != nil {
		return fmt.Errorf("error getting home directory: %w", err)
	}

	configFile := filepath.Join(home, ".config", "snappub", "config.yaml")
	if err := viper.WriteConfigAs(configFile); err != nil {
		return fmt.Errorf("error writing config: %w", err)
	}

	fmt.Printf("✓ Key saved to config as appKeys.%d\n", userFid)
	fmt.Printf("\nYou can now use: snappub ping <url> --fid %d\n", userFid)

	return nil
}

// derivePrivateKey derives an Ethereum private key from a BIP-39 seed
// using the standard Ethereum derivation path m/44'/60'/0'/0/0
func derivePrivateKey(seed []byte) (*ecdsa.PrivateKey, error) {
	// Create master key from seed
	masterKey, err := hdkeychain.NewMaster(seed, &chaincfg.MainNetParams)
	if err != nil {
		return nil, fmt.Errorf("failed to create master key: %w", err)
	}

	// Derive path m/44'/60'/0'/0/0
	// m/44'
	purpose, err := masterKey.Derive(hdkeychain.HardenedKeyStart + 44)
	if err != nil {
		return nil, fmt.Errorf("failed to derive purpose: %w", err)
	}

	// m/44'/60'
	coinType, err := purpose.Derive(hdkeychain.HardenedKeyStart + 60)
	if err != nil {
		return nil, fmt.Errorf("failed to derive coin type: %w", err)
	}

	// m/44'/60'/0'
	account, err := coinType.Derive(hdkeychain.HardenedKeyStart + 0)
	if err != nil {
		return nil, fmt.Errorf("failed to derive account: %w", err)
	}

	// m/44'/60'/0'/0
	change, err := account.Derive(0)
	if err != nil {
		return nil, fmt.Errorf("failed to derive change: %w", err)
	}

	// m/44'/60'/0'/0/0
	addressKey, err := change.Derive(0)
	if err != nil {
		return nil, fmt.Errorf("failed to derive address key: %w", err)
	}

	// Get the private key
	privKey, err := addressKey.ECPrivKey()
	if err != nil {
		return nil, fmt.Errorf("failed to get EC private key: %w", err)
	}

	return privKey.ToECDSA(), nil
}

func pollForApproval(token string) (int, error) {
	maxAttempts := 120 // 10 minutes with 5 second intervals
	for i := 0; i < maxAttempts; i++ {
		time.Sleep(5 * time.Second)

		resp, err := http.Get(fmt.Sprintf("https://api.warpcast.com/v2/signed-key-request?token=%s", token))
		if err != nil {
			continue // Retry on error
		}

		var pollResp PollResponse
		if err := json.NewDecoder(resp.Body).Decode(&pollResp); err != nil {
			resp.Body.Close()
			continue
		}
		resp.Body.Close()

		state := pollResp.Result.SignedKeyRequest.State
		fmt.Printf(".")

		if state == "completed" {
			fmt.Println()
			return pollResp.Result.SignedKeyRequest.UserFid, nil
		}

		if state == "rejected" {
			return 0, fmt.Errorf("key request was rejected")
		}
	}

	return 0, fmt.Errorf("timeout waiting for approval")
}

func init() {
	rootCmd.AddCommand(appkeyCmd)
	appkeyCmd.AddCommand(generateCmd)
}
