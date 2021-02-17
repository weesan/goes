package goes

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"

	"github.com/weesan/goes/json"
)

type Goes struct {
	path    string
	indices Indices
}

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func NewGoes() *Goes {
	return &Goes{}
}

// Given an index name, find the pertinent index struct; or create one otherwise.
func (goes *Goes) findIndex(indexName string, created ...bool) *Index {
	if index, found := goes.indices[indexName]; found {
		return index
	}

	// If created is not given or is false, return empty index.
	if len(created) == 0 || created[0] == false {
		return nil
	}

	// Otherwise, create one if it doesn't exist.
	index, err := newIndex(fmt.Sprintf("%s/%s", goes.path, indexName))
	if err != nil {
		return nil
	}

	goes.indices[indexName] = index
	return index
}

func (goes *Goes) Init(path string) error {
	log.Printf("Loading indices from %s", path)

	// Check if the path exists.
	if _, err := os.Stat(path); os.IsNotExist(err) {
		log.Printf("GOES path, %s, doesn't exist, creating one.", path)
		if err := os.Mkdir(path, 0755); err != nil {
			return err
		}
	}

	// Scan for indices.
	files, err := ioutil.ReadDir(path)
	if err != nil {
		return err
	}

	indices := make(Indices, len(files))

	// Read in all the indices.
	for _, file := range files {
		index, err := newIndex(fmt.Sprintf("%s/%s", path, file.Name()))
		if err != nil {
			return err
		}
		indices[file.Name()] = index
	}

	goes.path = path
	goes.indices = indices
	return nil
}

func (goes *Goes) IndexJson(indexName string, id_field string, json_file string) error {
	index := goes.findIndex(indexName, true)
	if index == nil {
		log.Printf("Failed to find index: %s", indexName)
		return fmt.Errorf("Index not found: %s", indexName)
	}

	log.Printf("Indexing %s from %s", indexName, json_file)
	return index.indexJson(id_field, json_file)
}

func (goes *Goes) Search(indexName string, term string, size int) (json.Json, error) {
	log.Printf("Searching for %s on index %s", term, indexName)
	index := goes.findIndex(indexName)
	if index == nil {
		log.Printf("Failed to find index: %s", indexName)
		return nil, fmt.Errorf("Index not found: %s", indexName)
	}

	return index.search(term, size)
}
