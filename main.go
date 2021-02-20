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

	"github.com/gorilla/mux"
	"github.com/weesan/goes/json"
	Goes "github.com/weesan/goes/pkg"
)

const GOES_HOME = "/tmp/goes"

var goes = Goes.NewGoes()

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

func search_handler(w http.ResponseWriter, r *http.Request) {
	log.Printf("%s %s %s %s\n", r.RemoteAddr, r.Method, r.URL, r.Proto)

	vars := mux.Vars(r)
	idx := vars["idx"]

	query := r.URL.Query()

	res, err := goes.Search(idx, getQuery(query), getSize(query))
	response(w, r, res, err)
}

func count_handler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	idx := vars["idx"]
	res, err := goes.Count(idx)
	response(w, r, res, err)
}

func cluster_handler(w http.ResponseWriter, r *http.Request) {
	var res json.Json
	vars := mux.Vars(r)

	switch vars["cmd"] {
	case "health":
		log.Printf("Getting cluster health")
		res = json.Json{
			"cluster_name": "weesan-goes",
			"status":       "green",
			"timed_out":    false,
		}
	}

	response(w, r, res, nil)
}

func cat_handler(w http.ResponseWriter, r *http.Request) {
	var res string
	vars := mux.Vars(r)

	switch vars["cmd"] {
	case "indices":
		log.Printf("Catting indices")
		// TODO: more needs to be done here.
		res = "index          health status pri rep docs.count docs.deleted store.size pri.store.size\n"
		for idx, index := range goes.Indices() {
			res += fmt.Sprintf("%-14s %-6s %-6s %3d %3d %10d\n",
				idx, "green", "open", 1, 0, index.Count()["count"])
		}
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
	data := make(map[string]string, 0)
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
			data[id] = line
		}
	}
	goes.Index(idx, data)
}

func serve(server string, port int) {
	router := mux.NewRouter()
	router.HandleFunc("/{idx}/_search", search_handler).Methods("GET")
	router.HandleFunc("/{idx}/_count", count_handler).Methods("GET")
	router.HandleFunc("/_cluster/{cmd}", cluster_handler).Methods("GET")
	router.HandleFunc("/_cat/{cmd}", cat_handler).Methods("GET")
	router.HandleFunc("/_bulk", bulk_handler).Methods("POST")

	log.Printf("Listen on %s:%d", server, port)
	log.Fatal(http.ListenAndServe(fmt.Sprintf("%s:%d", server, port), router))
}

func main() {
	db_flag := flag.String("db", GOES_HOME, "Path to the database")
	idx_flag := flag.String("i", "", "Index name")
	id_flag := flag.String("id", "", "Id field")
	server_flag := flag.String("s", "localhost", "Server address")
	port_flag := flag.Int("p", 8080, "Port #")
	flag.Parse()

	if err := goes.Init(*db_flag); err != nil {
		log.Fatal(err)
		return
	}

	if *idx_flag != "" {
		// Create a new index.
		json_file := flag.Arg(0)
		if err := goes.IndexJson(*idx_flag, *id_flag, json_file); err != nil {
			log.Fatal(err)
		}
	} else {
		// Serve HTTP requests.
		serve(*server_flag, *port_flag)
	}
}
