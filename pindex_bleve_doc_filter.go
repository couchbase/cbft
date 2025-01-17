//  Copyright 2024-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package cbft

import (
	"encoding/json"
	"fmt"
	"math"
	"time"

	"github.com/blevesearch/bleve/v2/analysis"
	"github.com/blevesearch/bleve/v2/analysis/datetime/optional"
	"github.com/blevesearch/bleve/v2/mapping"
	"github.com/blevesearch/bleve/v2/numeric"
	"github.com/blevesearch/bleve/v2/search/query"
)

const maxFilterDepth = 5
const defaultInclusiveLowerBound = true
const defaultInclusiveUpperBound = false

// IndexFilter is an interface to specify a filter that
// can be used to filter documents before indexing
type IndexFilter interface {
	// A filter must be able to validate itself and return an error if it is
	// invalid.
	Validate(indexMapping mapping.IndexMapping, checkPriority bool, curDepth int) error

	// A filter must be able to determine if a document satisfies the filter.
	// The documentObj is the document to be indexed. The filter must return
	// true if the document satisfies the filter, and false otherwise.
	// If the documentObj does not have the field specified in the filter, the
	// filter must return false.
	IsSatisfied(documentObj interface{}) bool

	// All filters must return a priority. This is used to determine the order
	// in which filters are applied. Filters with higher priority are applied
	// first. The priority must be a non-negative integer. The lower the order of the filter,
	// the higher the priority.
	GetOrder() int
}

// -----------------------------------------------------------------------------

type TermFilter struct {
	Term  string `json:"term"`
	Field string `json:"field"`
	Order *int   `json:"order,omitempty"`
}

func (tf *TermFilter) Validate(indexMapping mapping.IndexMapping, checkPriority bool, curDepth int) error {
	// check if the filter has a valid order
	if checkPriority && (tf.Order == nil || *tf.Order < 0) {
		return fmt.Errorf("term filter has invalid order or is not specified")
	}
	// check if the filter depth has exceeded the max allowed depth
	if curDepth > maxFilterDepth {
		return fmt.Errorf("term filter has exceeded max allowed filter depth of %v", maxFilterDepth)
	}
	// term must be specified
	if tf.Term == "" {
		return fmt.Errorf("term filter must specify true or false")
	}
	// field must be specified
	if tf.Field == "" {
		return fmt.Errorf("term filter must specify the term")
	}
	return nil
}

func (tf *TermFilter) IsSatisfied(documentObj interface{}) bool {
	// Extract value of the filter's field from the document
	property := lookupPropertyPath(documentObj, tf.Field, true)
	if property == nil {
		// If the field is not present in the document,
		// the document does not satisfy the filter
		return false
	}
	// First, try to retrieve the field value as a single string
	if fieldVal, ok := mustString(property); ok {
		// Check if the single string matches the term in the filter
		return fieldVal == tf.Term
	} else if fieldValSlice, ok := mustStringSlice(property); ok {
		// If it's a slice of strings, check if at least one matches the term in the filter
		for _, val := range fieldValSlice {
			if val == tf.Term {
				return true
			}
		}
	}
	// If the field is not present in the document or
	// the field value does not match the filter's term,
	// the document does not satisfy the filter
	return false
}

func (tf *TermFilter) GetOrder() int {
	return *tf.Order
}

// -----------------------------------------------------------------------------

type DateRangeFilter struct {
	Start              string `json:"start,omitempty"`
	End                string `json:"end,omitempty"`
	InclusiveStart     *bool  `json:"inclusive_start,omitempty"`
	InclusiveEnd       *bool  `json:"inclusive_end,omitempty"`
	DateTimeParserName string `json:"datetime_parser,omitempty"`
	Field              string `json:"field"`
	Order              *int   `json:"order,omitempty"`
	// cache the actual parser object
	dateTimeParserObj analysis.DateTimeParser
	// store parsed start/end values to avoid parsing them for each document
	startNumeric int64
	endNumeric   int64
}

func (drf *DateRangeFilter) Validate(indexMapping mapping.IndexMapping, checkPriority bool, curDepth int) error {
	// check if the filter has a valid order
	if checkPriority && (drf.Order == nil || *drf.Order < 0) {
		return fmt.Errorf("date range filter has invalid order or is not specified")
	}
	// check if the filter depth has exceeded the max allowed depth
	if curDepth > maxFilterDepth {
		return fmt.Errorf("date range filter has exceeded max allowed filter depth of %v", maxFilterDepth)
	}
	// either start or end must be specified
	if drf.Start == "" && drf.End == "" {
		return fmt.Errorf("date range filter must specify at least one of start/end")
	}
	// field must be specified
	if drf.Field == "" {
		return fmt.Errorf("date range filter must specify field")
	}
	// validate the date time parser
	// if not specified, use the default parser
	dateTimeParserName := drf.DateTimeParserName
	if dateTimeParserName == "" {
		dateTimeParserName = optional.Name
	}
	parserObj := indexMapping.DateTimeParserNamed(dateTimeParserName)
	if parserObj == nil {
		return fmt.Errorf("date range filter specified unknown parser: %s", dateTimeParserName)
	}
	// cache the parser object
	drf.dateTimeParserObj = parserObj
	// pre-process the start and end values
	var min, max int64
	if drf.Start != "" {
		// parse the start time
		startTime, _, err := parserObj.ParseDateTime(drf.Start)
		if err != nil {
			return fmt.Errorf("date range filter err: %v, date time parser name: %s", err, dateTimeParserName)
		}
		min, err = parseTimeToInt(startTime)
		if err != nil {
			return fmt.Errorf("date range filter err: %v", err)
		}
	} else {
		// if start is not specified, set it to negative infinity
		min = numeric.Float64ToInt64(math.Inf(-1))
	}
	if drf.End != "" {
		// parse the end time
		endTime, _, err := parserObj.ParseDateTime(drf.End)
		if err != nil {
			return fmt.Errorf("date range filter err: %v, date time parser name: %s", err, dateTimeParserName)
		}
		max, err = parseTimeToInt(endTime)
		if err != nil {
			return fmt.Errorf("date range filter err: %v", err)
		}
	} else {
		// if end is not specified, set it to positive infinity
		max = numeric.Float64ToInt64(math.Inf(1))
	}
	// check if the start and end values are inclusive
	includeLowerBound := defaultInclusiveLowerBound
	if drf.InclusiveStart != nil {
		includeLowerBound = *drf.InclusiveStart
	}
	includeUpperBound := defaultInclusiveUpperBound
	if drf.InclusiveEnd != nil {
		includeUpperBound = *drf.InclusiveEnd
	}
	// adjust the start and end values based on the inclusive flags
	if !includeLowerBound && min != math.MaxInt64 {
		// if the start is exclusive, increment it by 1
		min++
	}
	if !includeUpperBound && max != math.MinInt64 {
		// if the end is exclusive, decrement it by 1
		max--
	}
	// store the pre-processed start and end values
	drf.startNumeric = min
	drf.endNumeric = max
	return nil
}

func (drf *DateRangeFilter) IsSatisfied(documentObj interface{}) bool {
	// Extract value of the filter's field from the document
	property := lookupPropertyPath(documentObj, drf.Field, true)
	if property == nil {
		// If the field is not present in the document,
		// the document does not satisfy the filter
		return false
	}
	// Inline function to handle date parsing and range checking
	checkDateInRange := func(fieldVal string) bool {
		dateVal, _, err := drf.dateTimeParserObj.ParseDateTime(fieldVal)
		if err != nil {
			return false
		}
		numericDate, err := parseTimeToInt(dateVal)
		if err != nil {
			return false
		}
		return numericDate >= drf.startNumeric && numericDate <= drf.endNumeric
	}
	// First, try to retrieve the field value as a single string
	if fieldVal, ok := mustString(property); ok {
		return checkDateInRange(fieldVal)
	} else if fieldValSlice, ok := mustStringSlice(property); ok {
		// If it's a slice of strings, check if at least one is in the date range
		for _, fieldVal := range fieldValSlice {
			if checkDateInRange(fieldVal) {
				return true
			}
		}
	}
	// If the field is not present in the document or
	// the field value is not in the date range,
	// the document does not satisfy the filter
	return false
}

func (drf *DateRangeFilter) GetOrder() int {
	return *drf.Order
}

func parseTimeToInt(t time.Time) (int64, error) {
	if t.Before(query.MinRFC3339CompatibleTime) || t.After(query.MaxRFC3339CompatibleTime) {
		// overflow / underflow
		return 0, fmt.Errorf("invalid/unsupported date range")
	}
	return t.UnixNano(), nil
}

// -----------------------------------------------------------------------------

type NumericRangeFilter struct {
	Min          *float64 `json:"min,omitempty"`
	Max          *float64 `json:"max,omitempty"`
	InclusiveMin *bool    `json:"inclusive_min,omitempty"`
	InclusiveMax *bool    `json:"inclusive_max,omitempty"`
	Field        string   `json:"field"`
	Order        *int     `json:"order,omitempty"`
	// cache the valid min and max values and inclusive flags
	min               float64
	max               float64
	includeLowerBound bool
	includeUpperBound bool
}

func (nrf *NumericRangeFilter) Validate(indexMapping mapping.IndexMapping, checkPriority bool, curDepth int) error {
	// check if the filter has a valid order
	if checkPriority && (nrf.Order == nil || *nrf.Order < 0) {
		return fmt.Errorf("numeric range filter has invalid order or is not specified")
	}
	// check if the filter depth has exceeded the max allowed depth
	if curDepth > maxFilterDepth {
		return fmt.Errorf("numeric range filter has exceeded max allowed filter depth of %v", maxFilterDepth)
	}
	// either min or max must be specified
	if nrf.Min == nil && nrf.Min == nrf.Max {
		return fmt.Errorf("numeric range filter must specify min or max")
	}
	// field must be specified
	if nrf.Field == "" {
		return fmt.Errorf("numeric range filter must specify field")
	}
	// create a cache with valid min and max values and inclusive flags
	min := math.Inf(-1)
	if nrf.Min != nil {
		min = *nrf.Min
	}
	max := math.Inf(1)
	if nrf.Max != nil {
		max = *nrf.Max
	}
	includeLowerBound := defaultInclusiveLowerBound
	if nrf.InclusiveMin != nil {
		includeLowerBound = *nrf.InclusiveMin
	}
	includeUpperBound := defaultInclusiveUpperBound
	if nrf.InclusiveMax != nil {
		includeUpperBound = *nrf.InclusiveMax
	}
	nrf.min = min
	nrf.max = max
	nrf.includeLowerBound = includeLowerBound
	nrf.includeUpperBound = includeUpperBound
	return nil
}

func (nrf *NumericRangeFilter) IsSatisfied(documentObj interface{}) bool {
	// Extract value of the filter's field from the document
	property := lookupPropertyPath(documentObj, nrf.Field, true)
	if property == nil {
		// If the field is not present in the document,
		// the document does not satisfy the filter
		return false
	}
	// inline function to handle numeric range checking
	checkNumericInRange := func(val float64) bool {
		return (val > nrf.min || (nrf.includeLowerBound && val == nrf.min)) &&
			(val < nrf.max || (nrf.includeUpperBound && val == nrf.max))
	}
	// First, try to retrieve the field value as a single numeric value
	if fieldVal, ok := mustNumeric(property); ok {
		// Check if the single numeric value is in the range
		return checkNumericInRange(fieldVal)
	} else if fieldValSlice, ok := mustNumericSlice(property); ok {
		// If it's a slice of numeric values, check if at least one is in the range
		for _, val := range fieldValSlice {
			if checkNumericInRange(val) {
				return true
			}
		}
	}
	// If the field is not present in the document or
	// the field value is not in the numeric range,
	// the document does not satisfy the filter
	return false
}

func (nrf *NumericRangeFilter) GetOrder() int {
	return *nrf.Order
}

// -----------------------------------------------------------------------------

type BooleanFieldFilter struct {
	Bool  *bool  `json:"bool"`
	Field string `json:"field"`
	Order *int   `json:"order,omitempty"`
}

func (bf *BooleanFieldFilter) Validate(indexMapping mapping.IndexMapping, checkPriority bool, curDepth int) error {
	// check if the filter has a valid order
	if checkPriority && (bf.Order == nil || *bf.Order < 0) {
		return fmt.Errorf("boolean field filter has invalid order or is not specified")
	}
	// check if the filter depth has exceeded the max allowed depth
	if curDepth > maxFilterDepth {
		return fmt.Errorf("boolean field filter has exceeded max allowed filter depth of %v", maxFilterDepth)
	}
	// boolean value must be specified
	if bf.Bool == nil {
		return fmt.Errorf("boolean field filter must specify true or false")
	}
	// field must be specified
	if bf.Field == "" {
		return fmt.Errorf("boolean field filter must specify field")
	}
	return nil
}

func (bf *BooleanFieldFilter) IsSatisfied(documentObj interface{}) bool {
	// Extract value of the filter's field from the document
	property := lookupPropertyPath(documentObj, bf.Field, true)
	if property == nil {
		// If the field is not present in the document,
		// the document does not satisfy the filter
		return false
	}
	// First, try to retrieve the field value as a single boolean value
	if fieldVal, ok := mustBool(property); ok {
		// Check if the single boolean value matches the filter's boolean value
		return fieldVal == *bf.Bool
	} else if fieldValSlice, ok := mustBoolSlice(property); ok {
		// If it's a slice of strings, check if at least one
		for _, val := range fieldValSlice {
			if val == *bf.Bool {
				return true
			}
		}
	}
	// If the field is not present in the document or
	// the field value does not match the filter's term,
	// the document does not satisfy the filter
	return false
}

func (bf *BooleanFieldFilter) GetOrder() int {
	return *bf.Order
}

// -----------------------------------------------------------------------------

type ConjunctionFilter struct {
	Conjuncts []IndexFilter `json:"conjuncts"`
	Order     *int          `json:"order,omitempty"`
}

func (cf *ConjunctionFilter) Validate(indexMapping mapping.IndexMapping, checkPriority bool, curDepth int) error {
	// check if the filter has a valid order
	if checkPriority && (cf.Order == nil || *cf.Order < 0) {
		return fmt.Errorf("conjunction filter has has invalid order or is not specified")
	}
	// check if the filter depth has exceeded the max allowed depth
	if curDepth > maxFilterDepth {
		return fmt.Errorf("conjunction filter has exceeded max allowed filter depth of %v", maxFilterDepth)
	}
	// conjunction filter must have at least one conjunct
	if len(cf.Conjuncts) == 0 {
		return fmt.Errorf("conjunction filter has no conjuncts")
	}
	// validate each inner query
	for _, innerQuery := range cf.Conjuncts {
		err := innerQuery.Validate(indexMapping, false, curDepth+1)
		if err != nil {
			return err
		}
	}
	return nil
}

func (cf *ConjunctionFilter) IsSatisfied(documentObj interface{}) bool {
	// A conjunction filter is satisfied if all its inner queries are satisfied
	for _, innerQuery := range cf.Conjuncts {
		if !innerQuery.IsSatisfied(documentObj) {
			return false
		}
	}
	return true
}

func (cf *ConjunctionFilter) GetOrder() int {
	return *cf.Order
}

func (cf *ConjunctionFilter) UnmarshalJSON(data []byte) error {
	var tmp struct {
		Conjuncts []json.RawMessage `json:"conjuncts"`
		Order     *int              `json:"order"`
	}
	err := json.Unmarshal(data, &tmp)
	if err != nil {
		return err
	}
	cf.Conjuncts = make([]IndexFilter, len(tmp.Conjuncts))
	for i, conjunct := range tmp.Conjuncts {
		cf.Conjuncts[i], err = ParseDocumentFilter(conjunct)
		if err != nil {
			return err
		}
	}
	cf.Order = tmp.Order
	return nil
}

// -----------------------------------------------------------------------------

type DisjunctionFilter struct {
	Disjuncts []IndexFilter `json:"disjuncts"`
	Order     *int          `json:"order,omitempty"`
	Min       *int          `json:"min,omitempty"`
}

func (df *DisjunctionFilter) Validate(indexMapping mapping.IndexMapping, checkPriority bool, curDepth int) error {
	if checkPriority && (df.Order == nil || *df.Order < 0) {
		return fmt.Errorf("disjunction filter has has invalid order or is not specified")
	}
	if curDepth > maxFilterDepth {
		return fmt.Errorf("disjunction filter has exceeded max allowed filter depth of %v", maxFilterDepth)
	}
	if len(df.Disjuncts) == 0 {
		return fmt.Errorf("disjunction filter has no disjuncts")
	}
	if df.Min != nil && *df.Min > len(df.Disjuncts) {
		return fmt.Errorf("disjunction filter has fewer than the minimum number of clauses to satisfy")
	}
	for _, innerQuery := range df.Disjuncts {
		err := innerQuery.Validate(indexMapping, false, curDepth+1)
		if err != nil {
			return err
		}
	}
	return nil
}
func (cf *DisjunctionFilter) IsSatisfied(documentObj interface{}) bool {
	// A disjunction filter is satisfied if at least `min`` of its inner queries is satisfied
	// If `min` is not specified, it defaults to 1
	minReq := 1
	if cf.Min != nil {
		minReq = *cf.Min
	}
	for _, innerQuery := range cf.Disjuncts {
		if innerQuery.IsSatisfied(documentObj) {
			minReq--
		}
		if minReq == 0 {
			return true
		}
	}
	return false
}

func (cf *DisjunctionFilter) GetOrder() int {
	return *cf.Order
}

func (cf *DisjunctionFilter) UnmarshalJSON(data []byte) error {
	var tmp struct {
		Disjuncts []json.RawMessage `json:"disjuncts"`
		Order     *int              `json:"order"`
		Min       *int              `json:"min"`
	}
	err := json.Unmarshal(data, &tmp)
	if err != nil {
		return err
	}
	cf.Disjuncts = make([]IndexFilter, len(tmp.Disjuncts))
	for i, disjunct := range tmp.Disjuncts {
		cf.Disjuncts[i], err = ParseDocumentFilter(disjunct)
		if err != nil {
			return err
		}
	}
	cf.Order = tmp.Order
	cf.Min = tmp.Min
	return nil
}

var ErrAmbiguousDocFilter = fmt.Errorf("ambiguous document filter detected")

// -----------------------------------------------------------------------------

func ParseDocumentFilter(DocumentFilterBytes []byte) (rv IndexFilter, err error) {
	var tmp map[string]interface{}
	err = json.Unmarshal(DocumentFilterBytes, &tmp)
	if err != nil {
		return nil, err
	}
	// check if the filter is a date range filter
	_, hasStart := tmp["start"]
	_, hasEnd := tmp["end"]
	if hasStart || hasEnd {
		var df DateRangeFilter
		err = json.Unmarshal(DocumentFilterBytes, &df)
		if err != nil {
			return nil, err
		}
		rv = &df
	}
	// check if the filter is a boolean field filter
	_, hasBool := tmp["bool"]
	if hasBool {
		if rv != nil {
			return nil, ErrAmbiguousDocFilter
		}
		var bf BooleanFieldFilter
		err = json.Unmarshal(DocumentFilterBytes, &bf)
		if err != nil {
			return nil, err
		}
		rv = &bf
	}
	// check if the filter is a term filter
	_, hasTerm := tmp["term"]
	if hasTerm {
		if rv != nil {
			return nil, ErrAmbiguousDocFilter
		}
		var tf TermFilter
		err = json.Unmarshal(DocumentFilterBytes, &tf)
		if err != nil {
			return nil, err
		}
		rv = &tf
	}
	// a filter can have
	// - "conjuncts": for conjunction filter
	// - "disjuncts": for disjunction filter
	// - only "min" or only "max" or both: a numeric range filter
	// - "disjuncts" and "min": a disjunction filter with min
	// ambiguous filters which are invalid:
	// - "disjuncts" and "min" and "max"
	// - "conjuncts" and ("min" or "max")
	// - "conjuncts" and "disjuncts"
	// check if the filter is a conjunction filter
	_, hasConjuncts := tmp["conjuncts"]
	if hasConjuncts {
		if rv != nil {
			return nil, ErrAmbiguousDocFilter
		}
		var cf ConjunctionFilter
		err = json.Unmarshal(DocumentFilterBytes, &cf)
		if err != nil {
			return nil, err
		}
		rv = &cf
	}
	// check if the filter is a disjunction filter
	_, hasDisjuncts := tmp["disjuncts"]
	if hasDisjuncts {
		if rv != nil {
			return nil, ErrAmbiguousDocFilter
		}
		var df DisjunctionFilter
		err = json.Unmarshal(DocumentFilterBytes, &df)
		if err != nil {
			return nil, err
		}
		rv = &df
	}
	// check if the filter is a numeric range filter
	// after verifying that it is not a disjunction filter
	// as both have the min field
	_, hasMin := tmp["min"].(float64)
	_, hasMax := tmp["max"].(float64)
	if hasMin || hasMax {
		if rv != nil {
			// If a filter is already set, this filter must be a disjunction filter, and has "min" and does
			// not have "max", otherwise it is ambiguous
			// Check if the filter is a disjunction filter with min specified
			if _, isDisjunction := rv.(*DisjunctionFilter); !(isDisjunction && hasMin && !hasMax) {
				// filter is already set, and is not a disjunction filter with min specified
				return nil, ErrAmbiguousDocFilter
			}
			// If the filter is a disjunction filter with min specified, then it is a valid filter and we must
			// retain is as is and not overwrite it with a numeric range filter
		} else {
			// Parse as NumericRangeFilter if no prior filter
			var nf NumericRangeFilter
			err = json.Unmarshal(DocumentFilterBytes, &nf)
			if err != nil {
				return nil, err
			}
			rv = &nf
		}
	}
	if rv != nil {
		return rv, nil
	}
	return nil, fmt.Errorf("unknown filter type")
}

func mustString(data interface{}) (string, bool) {
	if data != nil {
		str, ok := data.(string)
		if ok {
			return str, true
		}
	}
	return "", false
}

func mustStringSlice(data interface{}) ([]string, bool) {
	if data != nil {
		switch v := data.(type) {
		case string:
			return []string{v}, true
		case []interface{}:
			rv := make([]string, 0, len(v))
			for _, value := range v {
				typeCast, ok := value.(string)
				if ok {
					rv = append(rv, typeCast)
				}
			}
			return rv, true
		default:
			return nil, false
		}

	}
	return nil, false
}

func mustNumeric(data interface{}) (float64, bool) {
	if data != nil {
		num, ok := data.(float64)
		if ok {
			return num, true
		}
	}
	return 0, false
}

func mustNumericSlice(data interface{}) ([]float64, bool) {
	if data != nil {
		switch v := data.(type) {
		case float64:
			return []float64{v}, true
		case []interface{}:
			rv := make([]float64, 0, len(v))
			for _, value := range v {
				typeCast, ok := value.(float64)
				if ok {
					rv = append(rv, typeCast)
				}
			}
			return rv, true
		default:
			return nil, false
		}

	}
	return nil, false
}

func mustBool(data interface{}) (bool, bool) {
	if data != nil {
		boolean, ok := data.(bool)
		if ok {
			return boolean, true
		}
	}
	return false, false
}

func mustBoolSlice(data interface{}) ([]bool, bool) {
	if data != nil {
		switch v := data.(type) {
		case bool:
			return []bool{v}, true
		case []interface{}:
			rv := make([]bool, 0, len(v))
			for _, value := range v {
				typeCast, ok := value.(bool)
				if ok {
					rv = append(rv, typeCast)
				}
			}
			return rv, true
		default:
			return nil, false
		}

	}
	return nil, false
}
