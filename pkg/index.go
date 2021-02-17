package goes

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"

	"github.com/weesan/goes/json"
)

type Index struct {
	path   string
	shards Shards
}

type Indices map[string]*Index

func newIndex(path string) (*Index, error) {
	// Check if the path exists, if not, create one.
	if _, err := os.Stat(path); os.IsNotExist(err) {
		log.Printf("Index, %s, doesn't exist, creating one.", path)
		if err := os.Mkdir(path, 0755); err != nil {
			return nil, err
		}

		// Go ahead and create a shard.
		// TODO: will need to determine default # of shards.
		shard := newShard(fmt.Sprintf("%s/0", path))
		shards := make(Shards, 1)
		shards["0"] = shard
		return &Index{path, shards}, nil
	}

	log.Printf("Loading shards from %s", path)

	// Scan the shards.
	files, err := ioutil.ReadDir(path)
	if err != nil {
		return nil, err
	}

	shards := make(Shards, len(files))

	for _, file := range files {
		shard := newShard(fmt.Sprintf("%s/%s", path, file.Name()))
		shards[file.Name()] = shard
	}

	return &Index{path, shards}, nil
}

func (index *Index) indexJson(id_field string, json_file string) error {
	for _, shard := range index.shards {
		if err := shard.indexJson(id_field, json_file); err != nil {
			return err
		}
	}

	return nil
}

func (index *Index) search(term string, size int) (json.Json, error) {
	log.Printf("Searching for %s", term)
	for _, shard := range index.shards {
		res, err := shard.search(term, size)
		if err != nil {
			return nil, err
		}
		// TODO: need to merge data from different shards.
		return res, nil
	}

	return nil, nil
}
