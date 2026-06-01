package goes

type TermFieldDef struct {
	Value string `json:"value"`
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
