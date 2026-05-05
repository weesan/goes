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

	// Update the from/offset based on the size and shard count.
	params.from = params.from / (params.size * len(index.shards))

	ch := make(chan []json.Json)
	for _, shard := range index.shards {
		go func(ch chan []json.Json, shard *Shard) {
			res, _ := shard.search(params)
			ch <- res
		}(ch, shard)
	}

	successful := 0
	results := make([]json.Json, 0)
	for range index.shards {
		r := <-ch
		if len(r) == 0 {
			continue
		}
		successful++
		results = append(results, r...)
	}

	// Sort the results.
	if params.sort == "" {
		sort.SliceStable(results, func(i, j int) bool {
			s1 := results[i]["_score"].(float64)
			s2 := results[j]["_score"].(float64)
			return s1 > s2
		})
	} else {
		sort.SliceStable(results, func(i, j int) bool {
			s1 := results[i][params.sort]
			s2 := results[j][params.sort]
			if s1 == nil || s2 == nil {
				log.Printf("Failed to sort by %s", params.sort)
				return false
			}

			if params.order == "desc" {
				return s1.(string) > s2.(string)
			} else {
				return s1.(string) < s2.(string)
			}
		})
	}

	// Truncate to the right size.
	if params.size > len(results) {
		params.size = len(results)
	}
	begin := params.from % (params.size * len(index.shards))
	end := min(begin+params.size, len(results))
	res := results[begin:end]

	took := time.Since(start)

	return json.Json{
		"took":      took.Microseconds(),
		"timed_out": false,
		"_shards": json.Json{
			"total":      len(index.shards),
			"successful": successful,
			"skipped":    0,
			"failed":     0,
		},
		"hits": json.Json{
			"total": json.Json{
				"value":    len(res),
				"relation": "eq",
			},
			"hits": res,
		},
	}, nil
}

func (index *index) lookup(id string) (json.Json, error) {
	shard := index.shards[uint(hash(id)%uint32(len(index.shards)))]

	return shard.lookup(id)
}
