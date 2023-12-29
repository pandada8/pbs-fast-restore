package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"

	"github.com/klauspost/compress/zstd"
	"github.com/mitchellh/mapstructure"
)

// https://pbs.proxmox.com/docs/file-formats.html

type FixedIndex struct {
	Magic         [8]byte
	UUID          [16]byte
	Created       int64
	IndexChecksum [32]byte
	Size          uint64
	ChunkSize     uint64
	Chunk         [][32]byte
}

var FIXEDINDEX_MAGIC = []byte{47, 127, 65, 237, 145, 253, 15, 205}

func NewFixedIndex(reader io.Reader) (ret FixedIndex, err error) {
	var payload struct {
		Magic         [8]byte
		UUID          [16]byte
		Created       int64
		IndexChecksum [32]byte
		Size          uint64
		ChunkSize     uint64
	}

	var buf [4096]byte
	reader.Read(buf[:])

	err = binary.Read(bytes.NewBuffer(buf[:]), binary.LittleEndian, &payload)

	mapstructure.Decode(payload, &ret)

	for {
		var data [32]byte
		err = binary.Read(reader, binary.LittleEndian, &data)
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			break
		}
		ret.Chunk = append(ret.Chunk, data)
	}

	return
}

var (
	MAGIC_UNENCRYPTED_UNCOMPRESSED_CHUNK = []byte{66, 171, 56, 7, 190, 131, 112, 161}
	MAGIC_UNENCRYPTED_COMPRESSED_CHUNK   = []byte{49, 185, 88, 66, 111, 182, 163, 127}
	zstdDecoder, _                       = zstd.NewReader(nil)
)

func readChunk(pathstr string, chunksize uint64) (payload []byte, err error) {
	// print(pathstr)
	_, err = os.Stat(pathstr)
	if err != nil && os.IsNotExist(err) {
		panic(err)
	}
	// read magic
	file, err := os.OpenFile(pathstr, os.O_RDONLY, 0)
	if err != nil {
		return
	}
	defer file.Close()

	var header struct {
		Magic [8]byte
		Crc32 [4]byte
	}
	err = binary.Read(file, binary.LittleEndian, &header)
	if err != nil {
		panic(err)
		return
	}

	inputs, err := io.ReadAll(file)
	if err != nil {
		return
	}

	calced := make([]byte, 4)
	binary.LittleEndian.PutUint32(calced, crc32.ChecksumIEEE(inputs))
	if !bytes.Equal(calced, header.Crc32[:]) {
		err = errors.New("crc32 mismatched!")
		return
	}

	if bytes.Equal(header.Magic[:], MAGIC_UNENCRYPTED_UNCOMPRESSED_CHUNK) {
		return inputs, nil
	} else if bytes.Equal(header.Magic[:], MAGIC_UNENCRYPTED_COMPRESSED_CHUNK) {
		payload = make([]byte, chunksize)
		_, err = zstdDecoder.DecodeAll(inputs, payload)
		if err != nil {
			return
		}
	} else {
		panic("unsupported encrypted chunk")
	}
	return
}
