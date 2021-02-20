package goes

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"

	"github.com/weesan/goes/json"
)

type Goes struct {
	home    string
	indices Indices
}

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func NewGoes() *Goes {
	return &Goes{}
}

// Given an index name, find the pertinent index struct; or create one otherwise.
func (goes *Goes) findIndex(idx string, created ...bool) *Index {
	if index, found := goes.indices[idx]; found {
		return index
	}

	// If created is not given or is false, return empty index.
	if len(created) == 0 || created[0] == false {
		return nil
	}

	// Otherwise, create one if it doesn't exist.
	index, err := newIndex(idx, goes.home)
	if err != nil {
		return nil
	}

	goes.indices[idx] = index
	return index
}

func (goes *Goes) Init(home string) error {
	log.Printf("Loading indices from %s", home)

	// Check if the home path exists.
	if _, err := os.Stat(home); os.IsNotExist(err) {
		log.Printf("GOES home, %s, doesn't exist, creating one.", home)
		if err := os.Mkdir(home, 0755); err != nil {
			return err
		}
	}

	// Scan for indices.
	files, err := ioutil.ReadDir(home)
	if err != nil {
		return err
	}

	indices := make(Indices, len(files))

	// Read in all the indices.
	for _, file := range files {
		idx := file.Name()
		index, err := newIndex(idx, home)
		if err != nil {
			return err
		}
		indices[file.Name()] = index
	}

	goes.home = home
	goes.indices = indices
	return nil
}

func (goes *Goes) Count(idx string) (json.Json, error) {
	log.Printf("Counting for index %s", idx)
	index := goes.findIndex(idx)
	if index == nil {
		log.Printf("Failed to find index: %s", idx)
		return nil, fmt.Errorf("Index not found: %s", idx)
	}

	return index.Count(), nil
}

func (goes *Goes) Index(idx string, kv map[string]string) error {
	index := goes.findIndex(idx, true)
	if index == nil {
		log.Printf("Failed to find index: %s", idx)
		return fmt.Errorf("Index not found: %s", idx)
	}

	//log.Printf("Indexing %s: %s", idx, kv)
	return index.index(kv)
}

func (goes *Goes) IndexJson(idx string, id_field string, json_file string) error {
	index := goes.findIndex(idx, true)
	if index == nil {
		log.Printf("Failed to find index: %s", idx)
		return fmt.Errorf("Index not found: %s", idx)
	}

	log.Printf("Indexing %s from %s", idx, json_file)
	return index.indexJson(id_field, json_file)
}

func (goes *Goes) Search(idx string, term string, size int) (json.Json, error) {
	log.Printf("Searching for %s from index %s", term, idx)
	index := goes.findIndex(idx)
	if index == nil {
		log.Printf("Failed to find index: %s", idx)
		return nil, fmt.Errorf("Index not found: %s", idx)
	}

	return index.search(term, size)
}

func (goes *Goes) Indices() Indices {
	return goes.indices
}
