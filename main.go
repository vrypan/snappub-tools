package main

import (
	"snappub-tools/commands"
)

var Version = "dev"

func main() {
	commands.Execute(Version)
}
