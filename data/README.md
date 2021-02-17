# Data

## Companies

Source: https://www.kaggle.com/usfundamentals/us-stocks-fundamentals?select=companies.json

To prepare:
```
$ unzip archive.zip -d /tmp
$ jq -c .[] /tmp/companies.json > companies.json
```

## Products

Source: https://www.kaggle.com/promptcloud/fashion-products-updated-december-2019-from-amazon

To prepare:
```
# Ruby code to convert ruby hash to json
$ cat hash2json.rb
require "json"

str = File.read("/tmp/marketing_sample_for_amazon_com-ecommerce__20191201_20191231__20k_data.json")
hash = eval(str)

puts JSON::generate(hash)

# Extract product info
$ unzip archive.zip -d /tmp
$ ruby hash2json.rb | jq -c .root.page[].record > products.json
```
