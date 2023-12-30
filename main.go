package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"flag"
	"fmt"
	"os"
	"path"
	"sync/atomic"
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
	force   = flag.Bool("force", false, "force")

	ALL_ZERO_MAGIC_SHA256 = []byte{187, 159, 141, 246, 20, 116, 210, 94, 113, 250, 0, 114, 35, 24, 205, 56, 115, 150, 202, 23, 54, 96, 94, 18, 72, 130, 28, 192, 222, 61, 58, 248}
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

	if uint64(fidx.ChunkSize)*uint64(len(fidx.Chunk)) != fidx.Size {
		panic("parse error")
	}

	var destFile *os.File
	if _, err := os.Stat(*dest); err == nil {
		if !*force {
			panic("use --force to overwrite file")
		}
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

	err = syscall.Fallocate(int(destFile.Fd()), 0x10, 0, int64(fidx.Size))
	if err != nil {
		panic(err)
	}

	defer destFile.Close()

	totalRead := new(uint64)
	totalWrite := new(uint64)

	bar := pb.New(len(fidx.Chunk))
	bar.Start()

	jobQueue := make(chan int, *workers)
	t, _ := tomb.WithContext(context.Background())

	for i := 0; i < *workers; i++ {
		t.Go(func() error {
			for index := range jobQueue {
				checksum := fidx.Chunk[index][:]
				if bytes.Equal(checksum, ALL_ZERO_MAGIC_SHA256) {
					bar.Increment()
					continue
				}
				checksumStr := hex.EncodeToString(checksum)
				chunkPath := path.Join(*chunks, checksumStr[:4], checksumStr)

				buf, read, err := readChunk(chunkPath, fidx.ChunkSize)
				atomic.AddUint64(totalRead, read)
				if err != nil {
					panic(err)
				}
				written, err := destFile.WriteAt(buf, int64(fidx.ChunkSize)*int64(index))
				if err != nil {
					panic(err)
				}
				atomic.AddUint64(totalWrite, uint64(written))
				bar.Increment()
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
	bar.Finish()
	fmt.Printf("time used: %s total read: %s total writen: %s\n", time.Since(started), humanize.IBytes(*totalRead), humanize.IBytes(*totalWrite))
}
