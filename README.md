snappub-tools are a set of tools that implement the basic functionality of [snappub](https://github.com/vrypan/snappub).

They are written in Go and provide a CLI interface for interacting with Farcaster via Snapchain.

## Installation

### Via Homebrew

```bash
brew install vrypan/tap/snappub
```

### From Source

```bash
go install github.com/vrypan/snappub-tools@latest
```

### Download Binaries

Download pre-built binaries for your platform from the [releases page](https://github.com/vrypan/snappub-tools/releases).

## Commands

### snappub version

Display the current version of snappub.

```bash
snappub version
```

### snappub config

- snappub config ls
- snappub config get <param>
- snappub config set <param> <value>

Will list/get/set params in ~/.config/snappub/config.yaml
Other subcommands will load these values and use them as defaults.

Values that must be set:
- node: node ip:port
- appKeys: map[fid] -> hex key

Example config (~/.config/snappub/config.yaml):
```yaml
node: localhost:2283
appKeys:
  "280": "0x1234...abcd"  # vrypan, fid=280
```

## Generating Application Keys

To generate new Farcaster application keys (signers), use the standalone [fc-appkey](https://github.com/vrypan/fc-appkey) tool.

### snappub ping

snappub ping <url> [--fid=<fid>|--fname=<fname>] [--appkey=<hex>] [--debug]

This command will post a new cast with empty body and parentUrl=<url>.

Options:
- Either `--fid` or `--fname` must be provided (but not both)
- If `--fname` is provided, the FID will be looked up via GetUsernameProof
- If `--appkey` is not provided, the key will be looked up from config using `appKeys.<fid>`
- `--debug` will show the full server response as JSON

### snappub updates

snappub updates [url] [--fid=<fid>|--fname=<fname>] [--results=<n>] [--since=<timestamp>]

This command lists casts by FID that have a parentUrl set.

Options:
- Either `--fid` or `--fname` must be provided (but not both)
- If `--fname` is provided, the FID will be looked up via GetUsernameProof
- If `[url]` is provided, only casts with that specific parentUrl will be shown
- If `[url]` is not provided, all casts with any parentUrl will be shown
- `--results` limits the number of results (default: 10)
- `--since` filters to show only casts after the specified timestamp (ISO 8601 format)

Output format: `<url> <timestamp in ISO 8601>`

Examples:
```bash
# Get all casts with parentUrl for a specific FID
snappub updates --fid 280

# Get casts with a specific parentUrl
snappub updates https://example.com --fname vrypan

# Get latest 20 results
snappub updates --fid 280 --results 20

# Get casts since a specific time
snappub updates --fid 280 --since 2025-10-27T10:00:00Z

# Get casts for a specific URL since a timestamp
snappub updates https://example.com --fid 280 --since 2025-10-27T10:00:00Z
```
