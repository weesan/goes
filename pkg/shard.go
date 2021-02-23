package goes

/*
 * Bleve tutorial:
 * - https://blevesearch.com/docs/Getting%20Started/
 *
 * Bleve docs:
 * - https://godoc.org/github.com/blevesearch/bleve
 *
 * Bleve examples:
 * - https://github.com/blevesearch/beer-search
 * - https://github.com/blevesearch/bleve-explorer
 *
 * Fast HTTP server:
 * - https://github.com/tidwall/evio
 */

import (
	"fmt"
	"log"
	"sync"

	"github.com/blevesearch/bleve/v2"
	index_api "github.com/blevesearch/bleve_index_api"
	"github.com/weesan/goes/json"
)

const shardBatchSize = 1024

type Shard struct {
	num        uint
	idx        string
	home       string
	db         bleve.Index
	batch      *bleve.Batch
	batchSize  int
	batchMutex *sync.Mutex
}

type Shards map[uint]*Shard

func newShard(num uint, idx string, home string) *Shard {
	path := fmt.Sprintf("%s/%s/%d", home, idx, num)
	db, err := bleve.Open(path)
	switch err {
	case bleve.ErrorIndexPathDoesNotExist:
		log.Printf("Create a new shard %s/%d", idx, num)
		mapping := bleve.NewIndexMapping()
		db, err = bleve.New(path, mapping)
	case nil:
		log.Printf("Load shard %s/%d", idx, num)
	}

	if err != nil {
		log.Fatal(err)
		return nil
	}

	return &Shard{
		num:        num,
		idx:        idx,
		home:       home,
		db:         db,
		batch:      nil,
		batchSize:  0,
		batchMutex: &sync.Mutex{},
	}
}

func (shard *Shard) close() {
	shard.db.Close()
}

func (shard *Shard) count() uint64 {
	if count, err := shard.db.DocCount(); err == nil {
		return count
	}
	return 0
}

// The format of data is as follows:
// { "id1": "json_str1", "id2": "json_str2", ... }
func (shard *Shard) index(data []json.Json) error {
	shard.batchMutex.Lock()
	defer shard.batchMutex.Unlock()

	// Batch index the data
	if shard.batch == nil {
		shard.batch = shard.db.NewBatch()
	}

	for _, v := range data {
		id := v["id"].(string)
		if shard.batchSize >= shardBatchSize {
			// Flush the current batch.
			shard.db.Batch(shard.batch)
			// Start a new batch.
			shard.batch = shard.db.NewBatch()
			// Reset the size.
			shard.batchSize = 0
		}

		// Index the data keyed by id.
		shard.batch.Index(id, v)
		// Keep track the batch size.
		shard.batchSize++
	}

	return nil
}

func (shard *Shard) refresh() {
	shard.batchMutex.Lock()
	defer shard.batchMutex.Unlock()

	if shard.batch == nil {
		return
	}

	// Flush the batch if available.
	shard.db.Batch(shard.batch)

	// Reset the batch after.
	shard.batch = nil
	shard.batchSize = 0
}

func (shard *Shard) search(term string, size int) ([]json.Json, error) {
	//query := bleve.NewMatchQuery("Nike")
	query := bleve.NewQueryStringQuery(term)
	search := bleve.NewSearchRequest(query)

	search_res, err := shard.db.Search(search)
	if err != nil {
		log.Printf("Failed to search: %v\n", err)
		return nil, err
	}

	res := []json.Json{}

	for i, hit := range search_res.Hits {
		// Bail when we hit the given size.
		if i == size {
			break
		}

		id := hit.ID
		doc, err := shard.db.Document(id)
		if err != nil {
			log.Println(err)
			return nil, err
		}

		source := make(json.Json, 0)
		doc.VisitFields(func(field index_api.Field) {
			key, value := string(field.Name()), string(field.Value())
			source[key] = value
		})
		res = append(res, json.Json{
			"_index":  shard.idx,
			"_shard":  shard.num,
			"_id":     id,
			"_score":  hit.Score,
			"_source": source,
		})
	}

	return res, nil
}
