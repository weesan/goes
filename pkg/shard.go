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
	"os"
	"sync"
	"time"

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

func (shard *Shard) delete() {
	shard.close()

	path := fmt.Sprintf("%s/%s/%d", shard.home, shard.idx, shard.num)
	err := os.RemoveAll(path)
	if err != nil {
		log.Println(err)
	}
}

func (shard *Shard) count() uint64 {
	if count, err := shard.db.DocCount(); err == nil {
		return count
	}
	return 0
}

var dateFormats = []string{
	time.RFC3339,
	time.RFC3339Nano,
	"2006-01-02T15:04:05",
	"2006-01-02 15:04:05",
	"2006-01-02",
	"2006/01/02",
}

func parseDate(s string) (time.Time, bool) {
	for _, fmt := range dateFormats {
		if t, err := time.Parse(fmt, s); err == nil {
			return t, true
		}
	}
	return time.Time{}, false
}

func detectDates(v json.Json) json.Json {
	out := make(json.Json, len(v))
	for k, val := range v {
		if len(k) > 0 && k[0] != '_' {
			if s, ok := val.(string); ok {
				if t, ok := parseDate(s); ok {
					out[k] = t
					continue
				}
			}
		}
		out[k] = val
	}
	return out
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
		if shard.batchSize >= shardBatchSize {
			// Flush the current batch.
			shard.db.Batch(shard.batch)
			// Start a new batch.
			shard.batch = shard.db.NewBatch()
			// Reset the size.
			shard.batchSize = 0
		}

		// Index the data keyed by id.
		id := v["_id"].(string)
		shard.batch.Index(id, detectDates(v))
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

func fieldValue(field index_api.Field) interface{} {
	switch f := field.(type) {
	case index_api.DateTimeField:
		if t, _, err := f.DateTime(); err == nil {
			return t.Format(time.RFC3339)
		}
	case index_api.NumericField:
		if n, err := f.Number(); err == nil {
			return n
		}
	case index_api.BooleanField:
		if b, err := f.Boolean(); err == nil {
			return b
		}
	}
	return string(field.Value())
}

func fieldESType(field index_api.Field) string {
	switch field.(type) {
	case index_api.DateTimeField:
		return "date"
	case index_api.NumericField:
		return "double"
	case index_api.BooleanField:
		return "boolean"
	default:
		return "text"
	}
}

func (shard *Shard) fields() (json.Json, error) {
	names, err := shard.db.Fields()
	if err != nil {
		return nil, err
	}

	// Sample one document to detect actual stored field types.
	fieldTypes := make(map[string]string)
	req := bleve.NewSearchRequest(bleve.NewMatchAllQuery())
	req.Size = 1
	if res, err := shard.db.Search(req); err == nil && len(res.Hits) > 0 {
		if doc, err := shard.db.Document(res.Hits[0].ID); err == nil && doc != nil {
			doc.VisitFields(func(field index_api.Field) {
				fieldTypes[string(field.Name())] = fieldESType(field)
			})
		}
	}

	properties := make(json.Json)
	for _, name := range names {
		esType := "text"
		if t, ok := fieldTypes[name]; ok {
			esType = t
		}
		properties[name] = json.Json{"type": esType}
	}
	return properties, nil
}

func (shard *Shard) docSource(id string) (json.Json, error) {
	doc, err := shard.db.Document(id)
	if err != nil {
		return nil, err
	}
	if doc == nil {
		log.Printf("Document %q found in index but has no stored fields", id)
		return json.Json{}, nil
	}

	source := make(json.Json)
	doc.VisitFields(func(field index_api.Field) {
		key := string(field.Name())
		val := fieldValue(field)
		if existing, ok := source[key]; ok {
			if arr, ok := existing.([]interface{}); ok {
				source[key] = append(arr, val)
			} else {
				source[key] = []interface{}{existing, val}
			}
		} else {
			source[key] = val
		}
	})
	return source, nil
}

func (shard *Shard) search(params *Params) ([]json.Json, uint64, error) {
	var searchReq *bleve.SearchRequest
	if params.q == "" {
		searchReq = bleve.NewSearchRequest(bleve.NewMatchAllQuery())
	} else {
		searchReq = bleve.NewSearchRequest(bleve.NewQueryStringQuery(params.q))
	}

	if params.sort != "" {
		sortField := params.sort
		if params.order == "desc" {
			sortField = "-" + params.sort
		}
		searchReq.SortBy([]string{sortField})
	} else if params.q == "" {
		searchReq.SortBy([]string{"-_id"})
	}

	searchReq.From = params.from
	searchReq.Size = params.size

	search_res, err := shard.db.Search(searchReq)
	if err != nil {
		log.Printf("Failed to search: %v\n", err)
		return nil, 0, err
	}

	res := []json.Json{}

	for i, hit := range search_res.Hits {
		// Bail when we hit the given size.
		if i == params.size {
			break
		}

		id := hit.ID
		source, err := shard.docSource(id)
		if err != nil {
			log.Println(err)
			return nil, 0, err
		}

		res = append(res, json.Json{
			"_index":  shard.idx,
			"_shard":  shard.num,
			"_id":     id,
			"_score":  hit.Score,
			"_source": source,
		})
	}

	return res, search_res.Total, nil
}

func (shard *Shard) lookup(id string) (json.Json, error) {
	source, err := shard.docSource(id)
	if err != nil {
		log.Println(err)
		return nil, err
	}

	if source == nil {
		return json.Json{
			"_index": shard.idx,
			"_shard": shard.num,
			"_id":    id,
			"found":  false,
		}, nil
	}

	return json.Json{
		"_index":  shard.idx,
		"_shard":  shard.num,
		"_id":     id,
		"found":   true,
		"_source": source,
	}, nil
}

func (shard *Shard) termCount(field, value string) uint64 {
	q := bleve.NewTermQuery(value)
	q.SetField(field)
	req := bleve.NewSearchRequest(q)
	req.Size = 0
	res, err := shard.db.Search(req)
	if err != nil {
		return 0
	}
	return res.Total
}
