package goes

import (
	"fmt"

	"github.com/blevesearch/bleve/v2"
	bleveQuery "github.com/blevesearch/bleve/v2/search/query"
)

func parseESQuery(body map[string]interface{}) (bleveQuery.Query, error) {
	for queryType, rawBody := range body {
		qBody, _ := rawBody.(map[string]interface{})
		switch queryType {
		case "bool":
			return parseBoolQuery(qBody)
		case "term":
			return parseTermQuery(qBody)
		case "match_all":
			return bleve.NewMatchAllQuery(), nil
		}
	}
	return nil, fmt.Errorf("unsupported or empty query")
}

func parseBoolQuery(body map[string]interface{}) (bleveQuery.Query, error) {
	var shouldClauses []bleveQuery.Query

	if shouldList, ok := body["should"].([]interface{}); ok {
		for _, item := range shouldList {
			clause, ok := item.(map[string]interface{})
			if !ok {
				continue
			}
			q, err := parseESQuery(clause)
			if err != nil {
				return nil, err
			}
			shouldClauses = append(shouldClauses, q)
		}
	}

	if len(shouldClauses) > 0 {
		dq := bleve.NewDisjunctionQuery(shouldClauses...)
		minMatch := 1.0
		if mm, ok := body["minimum_should_match"].(float64); ok {
			minMatch = mm
		}
		dq.SetMin(minMatch)
		return dq, nil
	}

	return bleve.NewMatchAllQuery(), nil
}

func parseTermQuery(body map[string]interface{}) (bleveQuery.Query, error) {
	for field, val := range body {
		var value string
		switch v := val.(type) {
		case string:
			value = v
		case map[string]interface{}:
			if s, ok := v["value"].(string); ok {
				value = s
			}
		}
		q := bleve.NewTermQuery(value)
		q.SetField(field)
		return q, nil
	}
	return nil, fmt.Errorf("empty term query")
}
