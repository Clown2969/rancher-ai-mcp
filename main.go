package main

//go:generate go run ./internal/toolsdoc

import (
	"github.com/Clown2969/rancher-ai-mcp/cmd"
)

func main() {
	cmd.Execute()
}
