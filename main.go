package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/weesan/goes/json"
	Goes "github.com/weesan/goes/pkg"
)

const GOES_HOME = "/tmp/goes"

var goes *Goes.Goes

func pretty(query url.Values) bool {
	res := false

	_, found := query["pretty"]
	if !found {
		return res
	}

	switch query.Get("pretty") {
	case "true":
		res = true
	case "false":
		res = false
	case "":
		res = true
	default:
		res = false
	}

	return res
}

func response(w http.ResponseWriter, r *http.Request, res json.Json, err error) {
	if err != nil {
		log.Println(err)
		http.Error(
			w,
			fmt.Sprintf("{\"error\": \"%s\"}", err),
			http.StatusBadRequest,
		)
		return
	}

	var j []byte

	if pretty(r.URL.Query()) {
		j, err = json.PrettyDumps(res)
	} else {
		j, err = json.Dumps(res)
	}
	if err != nil {
		log.Println(err)
		http.Error(
			w,
			fmt.Sprintf("{\"error\": \"failed to parse pretty\"}"),
			http.StatusBadRequest,
		)
		return
	}

	fmt.Fprintf(w, string(j)+"\n")
}

func getSize(query url.Values) int {
	size_str := query.Get("size")

	size, err := strconv.Atoi(size_str)
	if err != nil {
		// Return 10 by default.
		return 10
	}

	return size
}

func getQuery(query url.Values) string {
	q_str := query.Get("q")

	q, err := url.QueryUnescape(q_str)
	if err != nil {
		return ""
	}

	return q
}

func idx_handler(w http.ResponseWriter, r *http.Request) {
	log.Printf("%s %s %s %s\n", r.RemoteAddr, r.Method, r.URL, r.Proto)

	idx := r.PathValue("idx")
	cmd := r.PathValue("cmd")
	query := r.URL.Query()

	var res json.Json
	var err error

	switch cmd {
	case "_search":
		res, err = goes.Search(idx, getQuery(query), getSize(query))
	case "_count":
		res, err = goes.Count(idx)
	case "_refresh":
		res, err = goes.Refresh(idx)
	default:
		log.Printf("Unknown cmd: %s", cmd)
	}

	response(w, r, res, err)
}

func cluster_handler(w http.ResponseWriter, r *http.Request) {
	var res json.Json

	switch r.PathValue("cmd") {
	case "health":
		log.Printf("Getting cluster health")
		res = goes.ClusterHealth()
	}

	response(w, r, res, nil)
}

func cat_handler(w http.ResponseWriter, r *http.Request) {
	var res string

	switch r.PathValue("cmd") {
	case "indices":
		log.Printf("Catting indices")
		res = goes.CatIndices()
	case "nodes":
		log.Printf("Catting nodes")
		res = goes.CatNodes()
	}

	fmt.Fprintf(w, res)
}

func bulk_handler(w http.ResponseWriter, r *http.Request) {
	log.Printf("%s %s %s %s\n", r.RemoteAddr, r.Method, r.URL, r.Proto)

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Println(err)
		return
	}

	var op, idx, id string
	data := make([]json.Json, 0)
	for i, line := range strings.Split(string(body), "\n") {
		if i%2 == 0 {
			// Operations such as index, delete, create, update, etc.
			action := json.Loads(line)
			for k, v := range action {
				op = k
				// meta := v.(json.Json)
				meta := v.(map[string]interface{})
				idx = meta["_index"].(string)
				id = meta["_id"].(string)
				break
			}
		} else {
			// Data
			if false {
				log.Printf("%s %s %s", op, idx, id)
				log.Printf("Data: %s", line)
			}
			// Convert the json string to a map.
			j := json.Loads(line)
			j["id"] = id
			data = append(data, j)
		}
	}
	goes.Index(idx, data)
}

func refresh_handler(w http.ResponseWriter, r *http.Request) {
	res, err := goes.Refresh("*")
	response(w, r, res, err)
}

func delete_handler(w http.ResponseWriter, r *http.Request) {
	idx := r.PathValue("idx")
	res, err := goes.Delete(idx)
	response(w, r, res, err)
}

func serve(server string, port int) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{idx}/{cmd}", idx_handler)
	mux.HandleFunc("GET /_cluster/{cmd}", cluster_handler)
	mux.HandleFunc("GET /_cat/{cmd}", cat_handler)
	mux.HandleFunc("POST /_bulk", bulk_handler)
	mux.HandleFunc("GET /_refresh", refresh_handler)
	mux.HandleFunc("DELETE /{idx}", delete_handler)

	log.Printf("Listen on %s:%d", server, port)
	log.Fatal(http.ListenAndServe(fmt.Sprintf("%s:%d", server, port), mux))
}

func main() {
	cluster := flag.String("cluster", "GOES", "Name of the GOES cluster")
	home := flag.String("home", GOES_HOME, "Path to the database")
	nodeName := flag.String("node", "Node 1", "Node name")
	discovery := flag.String(
		"discovery", "239.1.1.1:9200", "Multicast address for node discovery")
	port := flag.Int("p", 8080, "Port #")
	server := flag.String("s", "localhost", "Server address")
	flag.Parse()

	var err error
	goes, err = Goes.NewGoes(*cluster, *nodeName, *home, *discovery)
	if err != nil {
		log.Fatal(err)
		return
	}

	// Serve HTTP requests.
	serve(*server, *port)
}
