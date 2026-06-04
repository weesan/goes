package goes

import "encoding/json"

// TermFieldDef accepts both {"value": "x"} and plain "x".
type TermFieldDef struct {
	Value string
}

func (t *TermFieldDef) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err == nil {
		t.Value = s
		return nil
	}
	var obj struct {
		Value string `json:"value"`
	}
	if err := json.Unmarshal(data, &obj); err != nil {
		return err
	}
	t.Value = obj.Value
	return nil
}

type FilterDef struct {
	Term map[string]TermFieldDef `json:"term"`
}

type FiltersAggDef struct {
	Filters        map[string]FilterDef `json:"filters"`
	OtherBucket    bool                 `json:"other_bucket"`
	OtherBucketKey string               `json:"other_bucket_key"`
	Keyed          bool                 `json:"keyed"`
}

type AggDef struct {
	Filters *FiltersAggDef `json:"filters"`
}
