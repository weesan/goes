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
	"bufio"
	"fmt"
	"log"
	"os"

	"github.com/blevesearch/bleve/v2"
	index_api "github.com/blevesearch/bleve_index_api"
	"github.com/weesan/goes/json"
)

const shardBatchSize = 1 << 16 // 65536

type Shard struct {
	num  uint
	idx  string
	home string
	db   bleve.Index
}

type Shards map[uint]*Shard

func newShard(num uint, idx string, home string) *Shard {
	path := fmt.Sprintf("%s/%s/%d", home, idx, num)
	db, err := bleve.Open(path)
	switch err {
	case bleve.ErrorIndexPathDoesNotExist:
		log.Printf("Creating new shard %s/%d", idx, num)

		mapping := bleve.NewIndexMapping()
		new_db, err := bleve.New(path, mapping)
		if err != nil {
			log.Fatal(err)
			return nil
		}

		return &Shard{num, idx, home, new_db}
	case nil:
		log.Printf("Loading shard %s/%d", idx, num)
		return &Shard{num, idx, home, db}
	default:
		log.Fatal(err)
		return nil
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
func (shard *Shard) index(kv map[string]string) error {
	// Batch index the data
	batch := shard.db.NewBatch()
	defer shard.db.Batch(batch)

	size := 0
	for id, line := range kv {
		strLen := len(line)
		if size+strLen >= shardBatchSize {
			// Flush the current batch.
			shard.db.Batch(batch)
			// Start a new batch.
			batch = shard.db.NewBatch()
			// Reset the size.
			size = 0
		}

		// Convert the json string to a map.
		data := json.Loads(line)
		// Index the data keyed by id.
		batch.Index(id, data)
	}

	return nil
}

func (shard *Shard) indexJson(id_field string, json_file string) error {
	log.Printf("Indexing json file %s ...", json_file)
	fp, err := os.Open(json_file)
	if err != nil {
		log.Println(err)
		return err
	}
	defer fp.Close()

	r := bufio.NewReader(fp)

	// Batch index some data
	batch := shard.db.NewBatch()
	defer shard.db.Batch(batch)
	size := 0

for_loop:
	for {
		switch line, err := r.ReadString('\n'); {
		case err != nil:
			break for_loop
		default:
			strLen := len(line)
			if size+strLen >= shardBatchSize {
				// Flush the current batch.
				shard.db.Batch(batch)
				// Start a new batch.
				batch = shard.db.NewBatch()
				// Reset the size.
				size = 0
			}

			data := json.Loads(line)

			var id string
			if id_field == "" {
				id = data["id"].(string)
			} else {
				id = data[id_field].(string)
			}
			batch.Index(id, data)
		}
	}

	return nil
}

func (shard *Shard) search(term string, size int) (json.Json, error) {
	//query := bleve.NewMatchQuery("Nike")
	query := bleve.NewQueryStringQuery(term)
	search := bleve.NewSearchRequest(query)

	search_res, err := shard.db.Search(search)
	if err != nil {
		log.Printf("Failed to search: %v\n", err)
		return nil, err
	}

	hits := []json.Json{}
	res := json.Json{
		"took":      search_res.Took.Microseconds(),
		"timed_out": false,
		"_shards": json.Json{
			"total":      1,
			"successful": 1,
			"skipped":    0,
			"failed":     0,
		},
		"hits": json.Json{
			"total": json.Json{
				"value":    len(search_res.Hits),
				"relation": "eq",
			},
			"hits": []json.Json{}, // Placeholder
		},
	}

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
		hits = append(hits, json.Json{
			"_index":  shard.idx,
			"_id":     id,
			"_score":  hit.Score,
			"_source": source,
		})
	}

	res["hits"].(json.Json)["hits"] = hits

	return res, nil
}
