package main

import (
	"flag"
	"github.com/mrdxy/raft-kv-example/storage/snapshot"
	"github.com/mrdxy/raft-kv-example/storage/wal"
	"log"
)

func main() {
	snapdir := flag.String("snap-dir", "data-1-snap", "snapshot directory")
	waldir := flag.String("wal-dir", "data-1-wal", "If set, dumps WAL from the informed path, rather than following the standard 'data_dir/member/wal/' location")
	flag.Parse()
	w := wal.NewReadOnlyFileWal(*waldir)
	snapshotter := snapshot.NewFileSnapshot(*snapdir)
	s, err := snapshotter.Load()
	st, ents, err := w.Read(s)
	if err != nil {
		log.Fatalf("error: %v\n", err)
	}
	log.Printf("state: %v", st.String())
	for i := range ents {
		log.Printf("data: %v", ents[i].String())
	}
}
