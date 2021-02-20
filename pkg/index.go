package goes

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strconv"

	"github.com/weesan/goes/json"
)

type Index struct {
	idx    string
	home   string
	shards Shards
}

type Indices map[string]*Index

func newIndex(idx string, home string) (*Index, error) {
	// Construct the path for the index.
	path := fmt.Sprintf("%s/%s", home, idx)

	// Check if the path exists, if not, create one.
	if _, err := os.Stat(path); os.IsNotExist(err) {
		log.Printf("Index %s doesn't exist, creating one.", idx)
		if err := os.Mkdir(path, 0755); err != nil {
			return nil, err
		}

		// Go ahead and create a shard.
		// TODO: will need to determine default # of shards.
		shard := newShard(0, idx, home)
		shards := make(Shards, 1)
		shards[0] = shard
		return &Index{idx, home, shards}, nil
	}

	log.Printf("Loading index %s", idx)

	// Scan the shards.
	files, err := ioutil.ReadDir(path)
	if err != nil {
		return nil, err
	}

	shards := make(Shards, len(files))

	for _, file := range files {
		num, err := strconv.ParseUint(file.Name(), 10, 64)
		if err != nil {
			log.Printf("Found a bad shard: %s", file.Name())
			continue
		}
		shard := newShard(uint(num), idx, home)
		shards[uint(num)] = shard
	}

	return &Index{idx, home, shards}, nil
}

func (index *Index) close() {
	for _, shard := range index.shards {
		shard.close()
	}
}

func (index *Index) Count() json.Json {
	total := uint64(0)
	for _, shard := range index.shards {
		count := shard.count()
		total += count
	}

	return json.Json{
		"count": total,
		"_shards": json.Json{
			"total":      1,
			"successful": 1,
			"skipped":    0,
			"failed":     0,
		},
	}
}

func (index *Index) index(kv map[string]string) error {
	for _, shard := range index.shards {
		if err := shard.index(kv); err != nil {
			return err
		}
	}

	return nil
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
