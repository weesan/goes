package goes

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"time"

	"github.com/weesan/goes/json"
)

const defaultIndexRefreshTimer = 2 * time.Second

const (
	GREEN = iota
	YELLOW
	RED
)

type status int

type Goes struct {
	cluster string
	home    string
	indices indices
	status  status
	nodes   nodes
}

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func (s status) String() string {
	return []string{"green", "yellow", "red"}[s]
}

func NewGoes(cluster, nodeName, home, discovery string) (*Goes, error) {
	// Check if the home path exists.
	if _, err := os.Stat(home); os.IsNotExist(err) {
		log.Printf("Create %s", home)
		if err := os.Mkdir(home, 0755); err != nil {
			return nil, err
		}
	} else {
		log.Printf("Load indices from %s", home)
	}

	// Scan for indices.
	files, err := ioutil.ReadDir(home)
	if err != nil {
		return nil, err
	}

	indices := make(indices, len(files))

	// Read in all the indices.
	for _, file := range files {
		idx := file.Name()
		index, err := newIndex(idx, home)
		if err != nil {
			return nil, err
		}
		indices[file.Name()] = index
	}

	// TODO: May move this to each individual index.
	refreshTicker := time.NewTicker(defaultIndexRefreshTimer)
	go func() {
		for {
			<-refreshTicker.C
			for _, index := range indices {
				index.refresh()
			}
		}
	}()

	// Manage the nodes.  Default is 1 node, the node itself.
	nodes := make(nodes, 1)
	node, err := newNode(nodeName, discovery)
	if err != nil {
		return nil, err
	}
	nodes[nodeName] = node

	return &Goes{
		cluster: cluster,
		home:    home,
		indices: indices,
		status:  GREEN,
		nodes:   nodes,
	}, nil
}

// Given an index name, find the pertinent index struct; or create one otherwise.
func (goes *Goes) findIndex(idx string, created ...bool) *index {
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

func (goes *Goes) Count(idx string) (json.Json, error) {
	log.Printf("Counting for index %s", idx)
	index := goes.findIndex(idx)
	if index == nil {
		log.Printf("Failed to find index: %s", idx)
		return nil, fmt.Errorf("Index not found: %s", idx)
	}

	return index.count(), nil
}

func (goes *Goes) Index(idx string, data []json.Json) error {
	index := goes.findIndex(idx, true)
	if index == nil {
		log.Printf("Failed to find index: %s", idx)
		return fmt.Errorf("Index not found: %s", idx)
	}

	//log.Printf("Indexing %s: %s", idx, kv)
	return index.index(data)
}

func (goes *Goes) Refresh(idx string) (json.Json, error) {
	total := 0
	if idx == "" {
		for _, index := range goes.indices {
			go index.refresh()
		}
		total = len(goes.indices)
	} else {
		index := goes.findIndex(idx)
		index.refresh()
		total = 1
	}

	return json.Json{
		"_shards": json.Json{
			"total":      total,
			"successful": total,
			"failed":     0,
		},
	}, nil
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

func (goes *Goes) ClusterHealth() json.Json {
	return json.Json{
		"cluster_name": goes.cluster,
		"status":       fmt.Sprint(goes.status),
		"timed_out":    false,
	}
}

func (goes *Goes) CatIndices() string {
	res := "index          health status pri rep docs.count docs.deleted store.size pri.store.size\n"
	for idx, index := range goes.indices {
		res += fmt.Sprintf("%-14s %-6s %-6s %3d %3d %10d\n",
			idx, "green", "open", len(index.shards), 0, index.count()["count"])
	}
	return res
}

func (goes *Goes) CatNodes() string {
	res := "ip        heap.percent ram.percent cpu load_1m load_5m load_15m node.role master name\n"
	for _, node := range goes.nodes {
		res += fmt.Sprintf("%-14s %-65s %-20s", node.ip, " ", node.name)
	}
	return res
}
