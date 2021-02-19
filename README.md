# GOES - GO-based ElasticSearch

The goal of GOES is to learn GO and use it to implement a drop-in
replacement for, at least close to it or a subset of, [Java-based
Elasticsearch](https://www.elastic.co/) at the API level.
[Bleve](https://github.com/blevesearch/bleve) is used as the backend
data store.

## To create a repo
```
$ go mod init github.com/weesan/goes
```

## To index from files
```
$ time go run main.go -i companies -id company_id data/companies.json
$ time go run main.go -i products  -id asin       data/products.json
```

## To start a server
```
$ go run main.go
```

## To search
```
$ curl -s 'localhost:8080/companies/_search?q=cisco&pretty&size=1'
$ curl -s 'localhost:8080/products/_search?q=title:shirts&pretty&size=2'
