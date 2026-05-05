package goes

import (
	"net/url"
	"strconv"
	"strings"
)

type Params struct {
	q     string
	size  int
	from  int
	sort  string
	order string
}

func getParam(query url.Values, param string) string {
	q_str := query.Get(param)

	q, err := url.QueryUnescape(q_str)
	if err != nil {
		return ""
	}

	return q
}

func getNum(query url.Values, param string, defaultValue int) int {
	num_str := getParam(query, param)

	num, err := strconv.Atoi(num_str)
	if err != nil {
		return defaultValue
	}

	return num
}

func NewParams(query url.Values) *Params {
	q := getParam(query, "q")
	size := getNum(query, "size", 10)
	from := getNum(query, "from", 0)
	sort := getParam(query, "sort")
	order := "desc"

	sorts := strings.Split(sort, ":")
	if len(sorts) == 2 {
		sort, order = sorts[0], sorts[1]
	}

	return &Params{q, size, from, sort, order}
}
