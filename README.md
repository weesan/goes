# GOES - GO-based ElasticSearch

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
```
