# GOES - GO-based ElasticSearch

The goal of GOES is to learn GO.  As an exercise, I'm going to
implement a drop-in replacement for, at least close to it or a subset
of, [Java-based Elasticsearch](https://github.com/elastic/elasticsearch)
at the API level.  [Bleve](https://github.com/blevesearch/bleve) is
used as the backend data store.

## To init a GO repo for dev

This is just a cheat sheet for myself for future GO projects.
Usually, one would start by cloing the repo as shown next.

```
$ go mod init github.com/weesan/goes
```

## To clone a repo
```
$ git clone https://github.com/weesan/goes
```

## To index documents stored in a JSON file via CLI

Before running the server, we need to index some data:
```
$ time go run main.go -i companies -id company_id data/companies.json
$ time go run main.go -i products  -id asin       data/products.json
```

Alternatively, despite slower, one can start a server, then use the
bulk API to index the data.

## To start a server
```
$ go run main.go
```

## To index documents stored in a JSON file via API

One can use [es](https://github.com/weesan/es_cli) to index the data:
```
$ time es -p 8080 index-json companies company_id < data/companies.json
$ time es -p 8080 index-json products  asin       < data/products.json
```

## To count
```
$ curl -s -XGET -H 'Content-Type: application/json' 'http://localhost:8080/companies/_count' | jq .count
$ curl -s -XGET -H 'Content-Type: application/json' 'http://localhost:8080/products/_count' | jq .count
```

Alternatively, one can use [es](https://github.com/weesan/es_cli) to do the same:
```
$ es -p 8080 count companies
$ es -p 8080 count products
```

## To search
```
$ curl -s 'localhost:8080/companies/_search?q=cisco&pretty&size=1'
$ curl -s 'localhost:8080/products/_search?q=title:shirts&pretty&size=2'
```

Alternatively, one can use [es](https://github.com/weesan/es_cli) to do the same:
```
$ es -p 8080 -z 1 grep cisco companies
$ es -p 8080 -z 2 grep title:shirts products
```

## To list all the indices
```
$ curl -s -XGET -H 'Content-Type: application/json' 'http://localhost:8080/_cat/indices?v&s=index&h=index,health,status,pri,rep,docs.count,docs.deleted,store.size,pri.store.size' | grep -v '^\.'
```

Alternatively, one can use [es](https://github.com/weesan/es_cli) to do the same:
```
$ es -p 8080 ls
```
