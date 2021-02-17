package json

import (
	"encoding/json"
)

type Json map[string]interface{}

func Loads(str string) Json {
	var res Json
	if err := json.Unmarshal([]byte(str), &res); err != nil {
		return nil
	}

	return res
}

func Dumps(j Json) ([]byte, error) {
	res, err := json.Marshal(j)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func PrettyDumps(j Json) ([]byte, error) {
	res, err := json.MarshalIndent(j, "", "  ")
	if err != nil {
		return nil, err
	}

	return res, nil
}
