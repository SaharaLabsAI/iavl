package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
)

func main() {
	os.Exit(run())
}

func run() int {
	root, err := RootCommand()
	if err != nil {
		return 1
	}

	ctx, cancel := context.WithCancel(context.Background())

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	defer func() {
		signal.Stop(signalChan)
		cancel()
	}()

	exitChan := make(chan int, 1)

	go func() {
		select {
		case <-signalChan:
			cancel()
		case <-ctx.Done():
		}

		<-signalChan
		select {
		case exitChan <- 2:
		default:
		}
	}()

	var execErr bool
	if err := root.ExecuteContext(ctx); err != nil {
		fmt.Printf("Error: %s\n", err.Error())
		execErr = true
	}

	select {
	case code := <-exitChan:
		return code
	default:
		if execErr {
			return 1
		}
		return 0
	}
}
