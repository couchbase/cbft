package cbft

import (
	"github.com/blevesearch/bleve/v2/analysis/datetime/optional"
	"github.com/blevesearch/bleve/v2/registry"
)

var cache = registry.NewCache()

const defaultDateTimeParser = optional.Name

type numericRange struct {
	Name string   `json:"name,omitempty"`
	Min  *float64 `json:"min,omitempty"`
	Max  *float64 `json:"max,omitempty"`
}

type dateTimeRange struct {
	Name  string  `json:"name,omitempty"`
	Start *string `json:"start,omitempty"`
	End   *string `json:"end,omitempty"`
}

// A facetRequest describes a facet or aggregation
// of the result document set you would like to be
// built.
type facetRequest struct {
	Size           int              `json:"size"`
	Field          string           `json:"field"`
	NumericRanges  []*numericRange  `json:"numeric_ranges,omitempty"`
	DateTimeRanges []*dateTimeRange `json:"date_ranges,omitempty"`
}

// facetsRequest groups together all the
// facetRequest objects for a single query.
type facetsRequest map[string]*facetRequest
