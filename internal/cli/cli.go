// Package cli implements the Mako command-line interface.
package cli

import (
	"fmt"
	"os"
)

const (
	Version = "0.1.0"
	Banner  = `
  ╔╦╗╔═╗╦╔═╔═╗
  ║║║╠═╣╠╩╗║ ║
  ╩ ╩╩ ╩╩ ╩╚═╝ 🦈
  Declarative Real-Time Data Pipelines
  v%s
`
)

func PrintBanner() {
	fmt.Fprintf(os.Stderr, Banner, Version)
}
