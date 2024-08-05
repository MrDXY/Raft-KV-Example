package main

import (
	"fmt"
	"github.com/mrdxy/raft-kv-example/server"
	"log"
	"os"

	"github.com/spf13/cobra"
)

var (
	id         int
	peerPort   int
	clientPort int
	peers      []string
)

func main() {
	var rootCmd = &cobra.Command{
		Use:   "start-server",
		Short: "Starts the server with specified configuration",
		Run: func(cmd *cobra.Command, args []string) {
			conf := &server.Config{
				Id:         id,
				Peers:      peers,
				PeerPort:   peerPort,
				ClientPort: clientPort,
			}
			s := server.NewServer(conf)
			s.Start()
			log.Printf("Server with ID %d started successfully on PeerPort %d and ClientPort %d\n", id, peerPort, clientPort)
		},
	}

	rootCmd.Flags().IntVar(&id, "id", 1, "Server ID")
	rootCmd.Flags().IntVar(&peerPort, "peer-port", 2380, "Peer Port")
	rootCmd.Flags().IntVar(&clientPort, "client-port", 2379, "Client Port")
	rootCmd.Flags().StringSliceVar(&peers, "peers", []string{"http://localhost:2380"}, "List of peer URLs")

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
