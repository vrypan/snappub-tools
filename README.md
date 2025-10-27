snappub-tools are a set of tools that implement the basic functionality of [snappub](https://github.com/vrypan/snappub).

They are written in Go. 

It will be a single command "snappub" with subcommands, so use cobra and viper.
Put the commands in a separate, commands/ folder.

We will also have to integrate with snapchain, so you will use the latest vrypan/farcaster-go/farcaster-go package.

Commands:

## snappub config

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

## snappub ping

snappub ping <url> [--fid=<fid>|--fname=<fname>] [--appkey=<hex>] [--debug]

This command will post a new cast with empty body and parentUrl=<url>.

Options:
- Either `--fid` or `--fname` must be provided (but not both)
- If `--fname` is provided, the FID will be looked up via GetUsernameProof
- If `--appkey` is not provided, the key will be looked up from config using `appKeys.<fid>`
- `--debug` will show the full server response as JSON
