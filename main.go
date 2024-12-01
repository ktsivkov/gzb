package main

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io"
	"log"
	"os"
	"strings"
)

func main() {
	// Generate uncompressed file
	contents, err := GetDataAsJsonl(Data())
	if err != nil {
		log.Fatal(err)
	}

	uncompressedFile, err := os.Create("uncompressed.jsonl")
	if err != nil {
		log.Fatal(err)
	}
	defer uncompressedFile.Close()

	if _, err := io.Copy(uncompressedFile, contents); err != nil {
		log.Fatal(err)
	}

	// Generate compressed file
	contents, err = GetDataAsJsonl(Data())
	if err != nil {
		log.Fatal(err)
	}

	compressedContents, err := Gzip(contents)
	if err != nil {
		log.Fatal(err)
	}

	compressedFile, err := os.Create("compressed.jsonl.gz")
	if err != nil {
		log.Fatal(err)
	}
	defer compressedFile.Close()

	if _, err := io.Copy(compressedFile, compressedContents); err != nil {
		log.Fatal(err)
	}
}

func Data() []interface{} {
	type tmpStruct struct {
		ItemSku         string `json:"item_sku"`
		Quantity        int    `json:"quantity"`
		FetchedQuantity int    `json:"fetched_quantity"`
	}
	return []interface{}{
		tmpStruct{"test-sku-1", 1, 1},
		tmpStruct{"test-sku-1", 1, 9},
		tmpStruct{"test-sku-3", 1, 10},
		tmpStruct{"test-sku-4", 2, 11},
	}
}

func GetDataAsJsonl(data []interface{}) (*bytes.Buffer, error) {
	var rows []string
	for _, rec := range data {
		jsonBytes, err := json.Marshal(rec)
		if err != nil {
			return nil, err
		}
		rows = append(rows, string(jsonBytes))
	}

	jsonl, err := Jsonl(rows)
	if err != nil {
		return nil, err
	}
	return jsonl, nil
}

func Jsonl(rows []string) (*bytes.Buffer, error) {
	buffer := &bytes.Buffer{}

	if _, err := buffer.WriteString(strings.Join(rows, "\n")); err != nil {
		return nil, err
	}

	return buffer, nil
}

func Gzip(reader io.Reader) (*bytes.Buffer, error) {
	buffer := &bytes.Buffer{}
	gzipWriter := gzip.NewWriter(buffer)
	defer func(gzipWriter *gzip.Writer) {
		err := gzipWriter.Close()
		if err != nil {
			log.Fatal(err)
		}
	}(gzipWriter)

	if _, err := io.Copy(gzipWriter, reader); err != nil {
		return nil, err
	}

	return buffer, nil
}
