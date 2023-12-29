package main

import (
	"context"
	"encoding/hex"
	"flag"
	"fmt"
	"os"
	"path"
	"syscall"
	"time"

	"github.com/cheggaaa/pb/v3"
	"github.com/dustin/go-humanize"
	"gopkg.in/tomb.v2"
)

var (
	source  = flag.String("src", "", "source file")
	chunks  = flag.String("chunks", "", ".chunks folder")
	workers = flag.Int("workers", 8, ".chunks folder")
	dest    = flag.String("dest", "", "dest file")
)

func main() {
	started := time.Now()
	flag.Parse()
	if *source == "" || *chunks == "" || *dest == "" {
		panic("you should provide -src, -chunks and -dest ")
	}

	file, err := os.OpenFile(*source, os.O_RDONLY, 0)
	if err != nil {
		panic(err)
	}
	fidx, err := NewFixedIndex(file)

	fmt.Printf("total size: %s chunks: %d chunk size: %s\n", humanize.IBytes(fidx.Size), len(fidx.Chunk), humanize.IBytes(fidx.ChunkSize))

	var destFile *os.File
	if _, err := os.Stat(*dest); err == nil {
		destFile, err = os.OpenFile(*dest, os.O_WRONLY, 0644)
		if err != nil {
			panic(err)
		}
	} else {
		fmt.Printf("create dest file: %s\n", *dest)
		destFile, err := os.OpenFile(*dest, os.O_WRONLY|os.O_CREATE, 0644)
		if err != nil {
			panic(err)
		}
		err = syscall.Fallocate(int(destFile.Fd()), 0, 0, int64(fidx.Size))
		if err != nil {
			panic(err)
		}
	}
	totalRead := new(uint64)

	bar := pb.New(len(fidx.Chunk))
	bar.Start()

	jobQueue := make(chan int, 0)
	// var fileLock sync.Mutex
	t, _ := tomb.WithContext(context.Background())

	for i := 0; i < *workers; i++ {
		t.Go(func() error {
			for index := range jobQueue {
				checksum := fidx.Chunk[index]
				checksumStr := hex.EncodeToString(checksum[:])
				chunkPath := path.Join(*chunks, checksumStr[:4], checksumStr)

				buf, err := readChunk(chunkPath, fidx.ChunkSize, totalRead)
				if err != nil {
					panic(err)
				}
				// fileLock.Lock()
				destFile.WriteAt(buf, int64(fidx.ChunkSize)*int64(index))
				bar.Increment()
				// fileLock.Unlock()
			}
			return nil
		})
	}

	t.Go(func() error {
		for i := 0; i < len(fidx.Chunk); i++ {
			jobQueue <- i
		}
		close(jobQueue)
		return nil
	})

	t.Wait()
	fmt.Printf("time used: %s total read: %s\n", time.Since(started), humanize.IBytes(*totalRead))
}
