package main

import (
	"flag"
	"fmt"
	_ "io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strconv"
	_ "strings"

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

func searchHandler(w http.ResponseWriter, r *http.Request) {
	log.Printf("%s %s %s %s\n", r.RemoteAddr, r.Method, r.URL, r.Proto)

	vars := mux.Vars(r)
	index_name := vars["index"]

	query := r.URL.Query()

	res, err := goes.Search(index_name, getQuery(query), getSize(query))
	response(w, r, res, err)
}

func countHandler(w http.ResponseWriter, r *http.Request) {
	log.Println("countHandler")
}

func clusterHandle(w http.ResponseWriter, r *http.Request) {
	log.Println("clusterHandle")
}

func catHandle(w http.ResponseWriter, r *http.Request) {
	log.Println("catHandle")
}

func bulkHandle(w http.ResponseWriter, r *http.Request) {
	log.Printf("%s %s %s %s\n", r.RemoteAddr, r.Method, r.URL, r.Proto)
}

func serve(server string, port int) {
	router := mux.NewRouter()
	router.HandleFunc("/{index}/_search", searchHandler).Methods("GET")
	router.HandleFunc("/{index}/_count", countHandler).Methods("GET")
	router.HandleFunc("/_cluster/{cmd}", clusterHandle).Methods("GET")
	router.HandleFunc("/_cat/{cmd}", catHandle).Methods("GET")
	router.HandleFunc("/_bulk", bulkHandle).Methods("POST")

	log.Printf("Listen on %s:%d", server, port)
	log.Fatal(http.ListenAndServe(fmt.Sprintf("%s:%d", server, port), router))
}

func main() {
	db_flag := flag.String("db", GOES_HOME, "Path to the database")
	index_flag := flag.String("i", "", "Index name")
	id_flag := flag.String("id", "", "Id field")
	server_flag := flag.String("s", "localhost", "Server address")
	port_flag := flag.Int("p", 8080, "Port #")
	flag.Parse()

	if err := goes.Init(*db_flag); err != nil {
		log.Fatal(err)
		return
	}

	if *index_flag != "" {
		// Create a new index
		json_file := flag.Arg(0)
		if err := goes.IndexJson(*index_flag, *id_flag, json_file); err != nil {
			log.Fatal(err)
		}
	} else {
		serve(*server_flag, *port_flag)
		/*
			// Search against the index.
			for _, arg := range flag.Args() {
				//search_term := strings.Replace(arg, ":", "\\:", -1)
				search_term := arg
				if res, err := goes.Search(search_term, *size_flag); err == nil {
					if j, err := json.Dumps(res); err != nil {
						log.Println(err)
						return
					} else {
						fmt.Println(string(j))
					}
				}
			}
		*/
	}
}
