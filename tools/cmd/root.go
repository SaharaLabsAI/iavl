package main

import (
	"github.com/spf13/cobra"
)

// nolint
func RootCommand() (*cobra.Command, error) {
	cmd := &cobra.Command{
		Use:   "iavl",
		Short: "benchmark cosmos/iavl",
	}
	// cmd.AddCommand(
	// 	gen.Command(),
	// 	snapshot.Command(),
	// 	rollback.Command(),
	// 	scan.Command(),
	// 	bench.Command(),
	// )
	return cmd, nil
}
