package goes

import (
	"fmt"
	"hash/fnv"
	"log"
	"os"
	"sort"
	"strconv"
	"time"

	"github.com/weesan/goes/json"
)

const defaultNumOfShardsPerIndex = 5

type index struct {
	idx    string
	home   string
	shards Shards
}

type indices map[string]*index

func hash(id string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(id))
	return h.Sum32()
}

func newIndex(idx string, home string) (*index, error) {
	// Construct the path for the index.
	path := fmt.Sprintf("%s/%s", home, idx)

	// Check if the path exists, if not, create one.
	if _, err := os.Stat(path); os.IsNotExist(err) {
		log.Printf("Create index %s", idx)
		if err := os.Mkdir(path, 0755); err != nil {
			return nil, err
		}

		// Go ahead and create a number of shards.
		shards := make(Shards, defaultNumOfShardsPerIndex)
		for i := uint(0); i < defaultNumOfShardsPerIndex; i++ {
			shards[i] = newShard(i, idx, home)
		}
		return &index{idx, home, shards}, nil
	}

	log.Printf("Load index %s", idx)

	// Scan the shards.
	files, err := os.ReadDir(path)
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

	return &index{idx, home, shards}, nil
}

func (index *index) close() {
	for _, shard := range index.shards {
		shard.close()
	}
}

func (index *index) delete() {
	done := make(chan bool, 0)
	for _, shard := range index.shards {
		go func(shard *Shard, done chan bool) {
			shard.delete()
			done <- true
		}(shard, done)
	}
	for range index.shards {
		<-done
	}

	path := fmt.Sprintf("%s/%s", index.home, index.idx)
	err := os.RemoveAll(path)
	if err != nil {
		log.Println(err)
	}
}

func (index *index) count() json.Json {
	total := uint64(0)
	count := make(chan uint64)

	// Gather the counts from different shards.
	for _, shard := range index.shards {
		go func(shard *Shard, count chan uint64) {
			count <- shard.count()
		}(shard, count)
	}

	// Sum up all the counts.
	for range index.shards {
		total += <-count
	}

	return json.Json{
		"count": total,
		"_shards": json.Json{
			"total":      len(index.shards),
			"successful": len(index.shards),
			"skipped":    0,
			"failed":     0,
		},
	}
}

func (index *index) index(data []json.Json) error {
	// Allocate some memory.
	buckets := make([][]json.Json, len(index.shards))
	for i := 0; i < len(index.shards); i++ {
		buckets[i] = make([]json.Json, 0)
	}

	// Sharding.
	for _, v := range data {
		id := v["_id"]
		bucket := hash(id.(string)) % uint32(len(index.shards))
		buckets[bucket] = append(buckets[bucket], v)
	}

	// Indexing.
	done := make(chan bool, 0)
	for i, shard := range index.shards {
		go func(shard *Shard, data []json.Json, done chan bool) error {
			err := shard.index(data)
			if err != nil {
				log.Printf("Failed to index shard %d: %v", shard.num, err)
			}
			done <- true
			return err
		}(shard, buckets[i], done)
	}
	for range index.shards {
		<-done
	}

	return nil
}

func (index *index) refresh() {
	for _, shard := range index.shards {
		go shard.refresh()
	}
}

func (index *index) search(params *Params) (json.Json, error) {
	start := time.Now()

	// Each shard starts from 0 and returns from+size candidates so the index
	// has enough results to satisfy global pagination.
	shardParams := *params
	shardParams.from = 0
	shardParams.size = params.from + params.size

	type shardResult struct {
		hits  []json.Json
		total uint64
	}

	ch := make(chan shardResult)
	for _, shard := range index.shards {
		go func(ch chan shardResult, shard *Shard) {
			hits, total, _ := shard.search(&shardParams)
			ch <- shardResult{hits, total}
		}(ch, shard)
	}

	var totalMatching uint64
	successful := 0
	results := make([]json.Json, 0)
	for range index.shards {
		r := <-ch
		totalMatching += r.total
		if len(r.hits) > 0 {
			successful++
			results = append(results, r.hits...)
		}
	}

	sort.SliceStable(results, func(i, j int) bool {
		if params.sort == "" {
			if params.q == "" && params.rawQuery == nil {
				return results[i]["_id"].(string) > results[j]["_id"].(string)
			}
			s1 := results[i]["_score"].(float64)
			s2 := results[j]["_score"].(float64)
			if s1 != s2 {
				return s1 > s2
			}
			return results[i]["_id"].(string) < results[j]["_id"].(string)
		}
		if params.sort == "_id" {
			id1 := results[i]["_id"].(string)
			id2 := results[j]["_id"].(string)
			if params.order == "desc" {
				return id1 > id2
			}
			return id1 < id2
		}
		si, _ := results[i]["_source"].(json.Json)
		sj, _ := results[j]["_source"].(json.Json)
		if si == nil || sj == nil {
			return false
		}
		v1, v2 := si[params.sort], sj[params.sort]
		if v1 == nil || v2 == nil {
			log.Printf("Failed to sort by %s: field missing", params.sort)
			return false
		}
		if params.order == "desc" {
			return v1.(string) > v2.(string)
		}
		return v1.(string) < v2.(string)
	})

	res := []json.Json{}
	if params.from < len(results) {
		end := min(params.from+params.size, len(results))
		res = results[params.from:end]
	}

	result := json.Json{
		"took":      time.Since(start).Microseconds(),
		"timed_out": false,
		"_shards": json.Json{
			"total":      len(index.shards),
			"successful": successful,
			"skipped":    0,
			"failed":     0,
		},
		"hits": json.Json{
			"total": json.Json{
				"value":    totalMatching,
				"relation": "eq",
			},
			"hits": res,
		},
	}

	if len(params.aggs) > 0 {
		result["aggregations"] = index.aggregate(params.aggs)
	}

	return result, nil
}

func (index *index) lookup(id string) (json.Json, error) {
	shard := index.shards[uint(hash(id)%uint32(len(index.shards)))]

	return shard.lookup(id)
}

func (index *index) mappings() (json.Json, error) {
	type result struct {
		props json.Json
		err   error
	}
	ch := make(chan result, len(index.shards))
	for _, shard := range index.shards {
		go func(shard *Shard) {
			props, err := shard.fields()
			ch <- result{props, err}
		}(shard)
	}

	merged := make(json.Json)
	for range index.shards {
		r := <-ch
		if r.err != nil {
			return nil, r.err
		}
		for k, v := range r.props {
			merged[k] = v
		}
	}

	return json.Json{
		"mappings": json.Json{
			"properties": merged,
		},
	}, nil
}

func (index *index) runFiltersAgg(filtersAgg *FiltersAggDef) json.Json {
	buckets := make(json.Json)
	for bucketName, filterDef := range filtersAgg.Filters {
		var total uint64
		for fieldName, termDef := range filterDef.Term {
			ch := make(chan uint64, len(index.shards))
			for _, shard := range index.shards {
				go func(shard *Shard, field, value string) {
					ch <- shard.termCount(field, value)
				}(shard, fieldName, termDef.Value)
			}
			for range index.shards {
				total += <-ch
			}
		}
		buckets[bucketName] = json.Json{"doc_count": total}
	}
	return json.Json{"buckets": buckets}
}

func (index *index) aggregate(aggs map[string]AggDef) json.Json {
	result := make(json.Json)
	for aggName, aggDef := range aggs {
		if aggDef.Filters != nil {
			result[aggName] = index.runFiltersAgg(aggDef.Filters)
		}
	}
	return result
}
