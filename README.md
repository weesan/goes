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

## To start a server
```
$ go run main.go
```

## To index documents stored in a JSON file

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

## Some examples with output

In one terminal, start the GOES server:
```
$ go run main.go
2021/03/03 13:12:49 goes.go:42: Create /tmp/goes
2021/03/03 13:12:49 main.go:201: Listen on localhost:8080
[The output below will be shown after indexing, catting and searching]
2021/03/03 13:12:54 main.go:147: 127.0.0.1:56055 POST /_bulk?pretty HTTP/1.1
2021/03/03 13:12:54 index.go:38: Create index companies
2021/03/03 13:12:54 shard.go:47: Create a new shard companies/0
2021/03/03 13:12:55 shard.go:47: Create a new shard companies/1
2021/03/03 13:12:55 shard.go:47: Create a new shard companies/2
2021/03/03 13:12:55 shard.go:47: Create a new shard companies/3
2021/03/03 13:12:55 shard.go:47: Create a new shard companies/4
2021/03/03 13:12:55 main.go:147: 127.0.0.1:56057 POST /_bulk?pretty HTTP/1.1
...
2021/03/03 13:12:55 main.go:147: 127.0.0.1:56059 POST /_bulk?pretty HTTP/1.1
2021/03/03 13:13:11 main.go:136: Catting indices
2021/03/03 13:14:37 main.go:99: 127.0.0.1:56208 GET /companies/_search?q=green&pretty&size=2 HTTP/1.1
2021/03/03 13:14:37 goes.go:162: Searching for green from index companies
```

In another terminal:
```
$ time es -p 8080 index-json companies company_id < data/companies.json

real    0m2.660s
user    0m0.527s
sys     0m0.192s

$ es -p 8080 ls
index          health status pri rep docs.count docs.deleted store.size pri.store.size
companies      green  open     5   0      12173

$ es -p 8080 -z 2 grep green companies
{
  "_shards": {
    "failed": 0,
    "skipped": 0,
    "successful": 5,
    "total": 5
  },
  "hits": {
    "hits": [
      {
        "_id": "1386278",
        "_index": "companies",
        "_score": 2.7946584386507096,
        "_shard": 4,
        "_source": {
          "company_id": "1386278",
          "id": "1386278",
          "name_latest": "Green DOT Corp"
        }
      },
      {
        "_id": "1168990",
        "_index": "companies",
        "_score": 2.7946584386507096,
        "_shard": 4,
        "_source": {
          "company_id": "1168990",
          "id": "1168990",
          "name_latest": "Superfund Green, L.P."
        }
      }
    ],
    "total": {
      "relation": "eq",
      "value": 2
    }
  },
  "timed_out": false,
  "took": 147
}
```

Please note that, currently, the unit of `took` in the example output
above is microseconds, not milliseconds as in the Java-based ES.
