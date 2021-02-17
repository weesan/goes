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
	"log"
	"os"

	"github.com/blevesearch/bleve/v2"
	index_api "github.com/blevesearch/bleve_index_api"
	"github.com/weesan/goes/json"
)

const shardBatchSize = 1 << 16 // 65536

type Shard struct {
	db bleve.Index
}

type Shards map[string]*Shard

func newShard(name string) *Shard {
	db, err := bleve.Open(name)
	switch err {
	case bleve.ErrorIndexPathDoesNotExist:
		log.Printf("Creating new shard %s ...", name)

		mapping := bleve.NewIndexMapping()
		new_db, err := bleve.New(name, mapping)
		if err != nil {
			log.Fatal(err)
			return nil
		}

		log.Printf("Loading new shard %s ...", name)
		return &Shard{new_db}
	case nil:
		log.Printf("Loading new shard %s ...", name)
		return &Shard{db}
	default:
		log.Fatal(err)
		return nil
	}
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

func (shard *Shard) search(term string, size int) (map[string]interface{}, error) {
	log.Printf("Searching for %s", term)
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
			name, value := string(field.Name()), string(field.Value())
			source[name] = value
		})
		hits = append(hits, json.Json{
			"_index":  "foo",
			"_id":     id,
			"_score":  hit.Score,
			"_source": source,
		})

		//log.Printf(" Score: %f, source: %s (%s)\n",
		//	hit.Score, source["store_name"], source["store_country"])
	}

	res["hits"].(json.Json)["hits"] = hits

	return res, nil
}
