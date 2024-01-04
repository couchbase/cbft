/*
Copyright 2017-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package cbft

import (
	"encoding/json"
	"fmt"
	"io"
	"unsafe"

	"github.com/couchbase/cbgt"

	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/geo"
	"github.com/blevesearch/bleve/v2/search"
	"github.com/blevesearch/bleve/v2/search/query"
	jsoniter "github.com/json-iterator/go"
)

// JSONImpl holds the custom json parser impl
var JSONImpl *CustomJSONImpl

// CustomJSONImpl enables the custom json parser
// is an implementation of JSONParserImplType
type CustomJSONImpl struct {
	CustomJSONImplType string
	//mgr instance for untime flipping through manager options
	mgr *cbgt.Manager
}

// SetManager sets the manager instance
func (p *CustomJSONImpl) SetManager(mgr *cbgt.Manager) {
	p.mgr = mgr
}

// GetManagerOption gets the requested manager option
func (p *CustomJSONImpl) GetManagerOption(key string) string {
	return p.mgr.GetOption(key)
}

// GetParserType returns the custom parser type
func (p *CustomJSONImpl) GetParserType() string {
	return p.CustomJSONImplType
}

// Unmarshal abstracts the underlying json lib used
func (p *CustomJSONImpl) Unmarshal(b []byte, v interface{}) error {
	return jsoniter.ConfigCompatibleWithStandardLibrary.Unmarshal(b, v)
}

// Decode abstracts the underlying json lib used
func (p *CustomJSONImpl) Decode(r io.Reader, v interface{}) error {
	return jsoniter.ConfigCompatibleWithStandardLibrary.NewDecoder(r).Decode(v)
}

// UnmarshalJSON abstracts the underlying json lib used
func UnmarshalJSON(b []byte, v interface{}) error {
	if JSONImpl != nil && JSONImpl.GetManagerOption("jsonImpl") != "std" {
		return JSONImpl.Unmarshal(b, v)
	}
	return json.Unmarshal(b, v)
}

func init() {
	// registers the custom json decoders with jsoniter
	registerCustomJSONDecoders()
}

func registerCustomJSONDecoders() {
	// adding all the custom decoders that bleve has implemented,
	// and need to extend as bleve introduces new custom decoders.
	jsoniter.RegisterTypeDecoderFunc("query.BleveQueryTime", decodeBleveQueryTime)

	jsoniter.RegisterTypeDecoderFunc("query.ConjunctionQuery", decodeConjunctionQuery)

	jsoniter.RegisterTypeDecoderFunc("query.BooleanQuery", decodeBooleanQuery)

	jsoniter.RegisterTypeDecoderFunc("query.GeoBoundingBoxQuery", decodeGeoBoundingBoxQuery)

	jsoniter.RegisterTypeDecoderFunc("query.GeoDistanceQuery", decodeGeoDistanceQuery)

	jsoniter.RegisterTypeDecoderFunc("query.DisjunctionQuery", decodeDisjunctionQuery)

	jsoniter.RegisterTypeDecoderFunc("bleve.IndexErrMap", decodeBleveIndexErrMap)

	jsoniter.RegisterTypeDecoderFunc("bleve.SearchRequest", decodeBleveSearchRequest)

	jsoniter.RegisterTypeDecoderFunc("bleve.FacetRequest", decodeFacetRequest)

	jsoniter.RegisterTypeDecoderFunc("query.MatchQueryOperator", decodeMatchQueryOperator)
}

func decodeBleveQueryTime(ptr unsafe.Pointer, iter *jsoniter.Iterator) {
	timeString := iter.ReadAny().ToString()
	dateTimeParser, err := cache.DateTimeParserNamed(query.QueryDateTimeParser)
	if err != nil {
		iter.Error = err
		return
	}
	t := query.BleveQueryTime{}
	t.Time, _, err = dateTimeParser.ParseDateTime(timeString)
	if err != nil {
		iter.Error = err
		return
	}
	*((*query.BleveQueryTime)(ptr)) = t
}

func decodeConjunctionQuery(ptr unsafe.Pointer, iter *jsoniter.Iterator) {
	tmp := struct {
		Conjuncts []json.RawMessage `json:"conjuncts"`
		Boost     *query.Boost      `json:"boost,omitempty"`
	}{}
	iter.ReadVal(&tmp)
	if iter.Error != nil {
		return
	}
	q := query.ConjunctionQuery{}
	q.Conjuncts = make([]query.Query, len(tmp.Conjuncts))
	for i, term := range tmp.Conjuncts {
		query1, err := parseQuery(term)
		if err != nil {
			iter.Error = err
			return
		}
		q.Conjuncts[i] = query1
	}
	q.BoostVal = tmp.Boost
	*((*query.ConjunctionQuery)(ptr)) = q
}

func decodeBooleanQuery(ptr unsafe.Pointer, iter *jsoniter.Iterator) {
	tmp := struct {
		Must    json.RawMessage `json:"must,omitempty"`
		Should  json.RawMessage `json:"should,omitempty"`
		MustNot json.RawMessage `json:"must_not,omitempty"`
		Boost   *query.Boost    `json:"boost,omitempty"`
	}{}
	iter.ReadVal(&tmp)
	if iter.Error != nil {
		return
	}
	var err error
	q := query.BooleanQuery{}
	if tmp.Must != nil {
		q.Must, err = parseQuery(tmp.Must)
		if err != nil {
			iter.Error = err
			return
		}
		_, isConjunctionQuery := q.Must.(*query.ConjunctionQuery)
		if !isConjunctionQuery {
			iter.Error = fmt.Errorf("must clause must be conjunction")
			return
		}
	}

	if tmp.Should != nil {
		q.Should, err = parseQuery(tmp.Should)
		if err != nil {
			iter.Error = err
			return
		}
		_, isDisjunctionQuery := q.Should.(*query.DisjunctionQuery)
		if !isDisjunctionQuery {
			iter.Error = fmt.Errorf("should clause must be disjunction")
			return
		}
	}

	if tmp.MustNot != nil {
		q.MustNot, err = parseQuery(tmp.MustNot)
		if err != nil {
			iter.Error = err
			return
		}
		_, isDisjunctionQuery := q.MustNot.(*query.DisjunctionQuery)
		if !isDisjunctionQuery {
			iter.Error = fmt.Errorf("must not clause must be disjunction")
			return
		}
	}

	q.BoostVal = tmp.Boost
	*((*query.BooleanQuery)(ptr)) = q
}

func decodeGeoBoundingBoxQuery(ptr unsafe.Pointer, iter *jsoniter.Iterator) {
	tmp := struct {
		TopLeft     interface{}  `json:"top_left,omitempty"`
		BottomRight interface{}  `json:"bottom_right,omitempty"`
		FieldVal    string       `json:"field,omitempty"`
		BoostVal    *query.Boost `json:"boost,omitempty"`
	}{}
	iter.ReadVal(&tmp)
	if iter.Error != nil {
		return
	}
	q := query.GeoBoundingBoxQuery{}
	// now use our generic point parsing code from the geo package
	lon, lat, found := geo.ExtractGeoPoint(tmp.TopLeft)
	if !found {
		iter.Error = fmt.Errorf("geo location top_left not in a valid format")
		return
	}
	q.TopLeft = []float64{lon, lat}
	lon, lat, found = geo.ExtractGeoPoint(tmp.BottomRight)
	if !found {
		iter.Error = fmt.Errorf("geo location bottom_right not in a valid format")
		return
	}
	q.BottomRight = []float64{lon, lat}
	q.FieldVal = tmp.FieldVal
	q.BoostVal = tmp.BoostVal
	*((*query.GeoBoundingBoxQuery)(ptr)) = q
}

func decodeGeoDistanceQuery(ptr unsafe.Pointer, iter *jsoniter.Iterator) {
	tmp := struct {
		Location interface{}  `json:"location,omitempty"`
		Distance string       `json:"distance,omitempty"`
		FieldVal string       `json:"field,omitempty"`
		BoostVal *query.Boost `json:"boost,omitempty"`
	}{}
	iter.ReadVal(&tmp)
	if iter.Error != nil {
		return
	}
	// now use our generic point parsing code from the geo package
	lon, lat, found := geo.ExtractGeoPoint(tmp.Location)
	if !found {
		iter.Error = fmt.Errorf("geo location not in a valid format")
		return
	}
	q := query.GeoDistanceQuery{}
	q.Location = []float64{lon, lat}
	q.Distance = tmp.Distance
	q.FieldVal = tmp.FieldVal
	q.BoostVal = tmp.BoostVal
	*((*query.GeoDistanceQuery)(ptr)) = q
}

func decodeDisjunctionQuery(ptr unsafe.Pointer, iter *jsoniter.Iterator) {
	tmp := struct {
		Disjuncts []json.RawMessage `json:"disjuncts"`
		Boost     *query.Boost      `json:"boost,omitempty"`
		Min       float64           `json:"min"`
	}{}
	iter.ReadVal(&tmp)
	if iter.Error != nil {
		return
	}
	q := query.DisjunctionQuery{}
	q.Disjuncts = make([]query.Query, len(tmp.Disjuncts))
	for i, term := range tmp.Disjuncts {
		query1, err := parseQuery(term)
		if err != nil {
			iter.Error = err
			return
		}
		q.Disjuncts[i] = query1
	}
	q.BoostVal = tmp.Boost
	q.Min = tmp.Min
	*((*query.DisjunctionQuery)(ptr)) = q
}

func decodeBleveIndexErrMap(ptr unsafe.Pointer, iter *jsoniter.Iterator) {
	var tmp map[string]string
	iter.ReadVal(&tmp)
	if iter.Error != nil {
		return
	}
	iem := make(bleve.IndexErrMap, 0)
	for k, v := range tmp {
		iem[k] = fmt.Errorf("%s", v)
	}
	*((*bleve.IndexErrMap)(ptr)) = iem
}

func decodeBleveSearchRequest(ptr unsafe.Pointer, iter *jsoniter.Iterator) {
	var temp struct {
		Q                json.RawMessage         `json:"query"`
		Size             *int                    `json:"size"`
		From             int                     `json:"from"`
		Highlight        *bleve.HighlightRequest `json:"highlight"`
		Fields           []string                `json:"fields"`
		Facets           bleve.FacetsRequest     `json:"facets"`
		Explain          bool                    `json:"explain"`
		Sort             []json.RawMessage       `json:"sort"`
		IncludeLocations bool                    `json:"includeLocations"`
		Score            string                  `json:"score"`
		KNN              json.RawMessage         `json:"knn"`
		KNNOperator      json.RawMessage         `json:"knn_operator"`
		PreSearchData    json.RawMessage         `json:"pre_search_data"`
	}

	r := &bleve.SearchRequest{}
	iter.ReadVal(&temp)
	if iter.Error != nil {
		return
	}

	var err error
	if temp.Size == nil {
		r.Size = 10
	} else {
		r.Size = *temp.Size
	}
	if temp.Sort == nil {
		r.Sort = search.SortOrder{&search.SortScore{Desc: true}}
	} else {
		r.Sort, err = search.ParseSortOrderJSON(temp.Sort)
		if err != nil {
			iter.Error = err
			return
		}
	}
	r.From = temp.From
	r.Explain = temp.Explain
	r.Highlight = temp.Highlight
	r.Fields = temp.Fields
	if temp.Facets != nil {
		r.Facets = make(map[string]*bleve.FacetRequest, len(temp.Facets))
		for k, fr := range temp.Facets {
			bfr := (*bleve.FacetRequest)(unsafe.Pointer(fr))
			r.AddFacet(k, bfr)
		}
	}

	r.IncludeLocations = temp.IncludeLocations
	r.Score = temp.Score
	r.Query, err = parseQuery(temp.Q)
	if err != nil {
		iter.Error = err
		return
	}

	if r.Size < 0 {
		r.Size = 10
	}
	if r.From < 0 {
		r.From = 0
	}
	if temp.PreSearchData != nil {
		r.PreSearchData, err = parsePreSearchData(temp.PreSearchData)
		if err != nil {
			iter.Error = err
			return
		}
	}

	if r, err = interpretKNNForRequest(temp.KNN, temp.KNNOperator, r); err != nil {
		return
	}

	*((*bleve.SearchRequest)(ptr)) = *r
}

func decodeFacetRequest(ptr unsafe.Pointer, iter *jsoniter.Iterator) {
	var temp facetRequest
	iter.ReadVal(&temp)
	if iter.Error != nil {
		return
	}
	fr := bleve.NewFacetRequest(temp.Field, temp.Size)
	for _, nr := range temp.NumericRanges {
		fr.AddNumericRange(nr.Name, nr.Min, nr.Max)
	}

	for _, dr := range temp.DateTimeRanges {
		fr.AddDateTimeRangeString(dr.Name, dr.Start, dr.End)
	}
	*((*bleve.FacetRequest)(ptr)) = *fr
}

func decodeMatchQueryOperator(ptr unsafe.Pointer, iter *jsoniter.Iterator) {
	var operatorString string
	iter.ReadVal(&operatorString)
	if iter.Error != nil {
		return
	}
	var o query.MatchQueryOperator
	switch operatorString {
	case "or":
		o = query.MatchQueryOperatorOr
	case "and":
		o = query.MatchQueryOperatorAnd
	default:
		iter.Error = fmt.Errorf("cannot unmarshal match operator '%v' from JSON", o)
		return
	}
	*((*query.MatchQueryOperator)(ptr)) = o
}

// parseQuery deserializes a JSON representation of
// a Query object.
func parseQuery(input []byte) (query.Query, error) {
	var tmp map[string]interface{}
	err := jsoniter.Unmarshal(input, &tmp)
	if err != nil {
		return nil, err
	}
	_, isMatchQuery := tmp["match"]
	_, hasFuzziness := tmp["fuzziness"]
	if hasFuzziness && !isMatchQuery {
		var rv query.FuzzyQuery
		err := jsoniter.Unmarshal(input, &rv)
		if err != nil {
			return nil, err
		}
		return &rv, nil
	}
	_, isTermQuery := tmp["term"]
	if isTermQuery {
		var rv query.TermQuery
		err := jsoniter.Unmarshal(input, &rv)
		if err != nil {
			return nil, err
		}
		return &rv, nil
	}
	if isMatchQuery {
		var rv query.MatchQuery
		err := jsoniter.Unmarshal(input, &rv)
		if err != nil {
			return nil, err
		}
		return &rv, nil
	}
	_, isMatchPhraseQuery := tmp["match_phrase"]
	if isMatchPhraseQuery {
		var rv query.MatchPhraseQuery
		err := jsoniter.Unmarshal(input, &rv)
		if err != nil {
			return nil, err
		}
		return &rv, nil
	}
	_, hasMust := tmp["must"]
	_, hasShould := tmp["should"]
	_, hasMustNot := tmp["must_not"]
	if hasMust || hasShould || hasMustNot {
		var rv query.BooleanQuery
		err := jsoniter.Unmarshal(input, &rv)
		if err != nil {
			return nil, err
		}
		return &rv, nil
	}
	_, hasTerms := tmp["terms"]
	if hasTerms {
		var rv query.PhraseQuery
		err := jsoniter.Unmarshal(input, &rv)
		if err != nil {
			// now try multi-phrase
			var rv2 query.MultiPhraseQuery
			err = jsoniter.Unmarshal(input, &rv2)
			if err != nil {
				return nil, err
			}
			return &rv2, nil
		}
		return &rv, nil
	}
	_, hasConjuncts := tmp["conjuncts"]
	if hasConjuncts {
		var rv query.ConjunctionQuery
		err := jsoniter.Unmarshal(input, &rv)
		if err != nil {
			return nil, err
		}
		return &rv, nil
	}
	_, hasDisjuncts := tmp["disjuncts"]
	if hasDisjuncts {
		var rv query.DisjunctionQuery
		err := jsoniter.Unmarshal(input, &rv)
		if err != nil {
			return nil, err
		}
		return &rv, nil
	}

	_, hasSyntaxQuery := tmp["query"]
	if hasSyntaxQuery {
		var rv query.QueryStringQuery
		err := jsoniter.Unmarshal(input, &rv)
		if err != nil {
			return nil, err
		}
		return &rv, nil
	}
	_, hasMin := tmp["min"].(float64)
	_, hasMax := tmp["max"].(float64)
	if hasMin || hasMax {
		var rv query.NumericRangeQuery
		err := jsoniter.Unmarshal(input, &rv)
		if err != nil {
			return nil, err
		}
		return &rv, nil
	}
	_, hasMinStr := tmp["min"].(string)
	_, hasMaxStr := tmp["max"].(string)
	if hasMinStr || hasMaxStr {
		var rv query.TermRangeQuery
		err := jsoniter.Unmarshal(input, &rv)
		if err != nil {
			return nil, err
		}
		return &rv, nil
	}
	_, hasStart := tmp["start"]
	_, hasEnd := tmp["end"]
	if hasStart || hasEnd {
		var rv query.DateRangeQuery
		err := jsoniter.Unmarshal(input, &rv)
		if err != nil {
			return nil, err
		}
		return &rv, nil
	}
	_, hasPrefix := tmp["prefix"]
	if hasPrefix {
		var rv query.PrefixQuery
		err := jsoniter.Unmarshal(input, &rv)
		if err != nil {
			return nil, err
		}
		return &rv, nil
	}
	_, hasRegexp := tmp["regexp"]
	if hasRegexp {
		var rv query.RegexpQuery
		err := jsoniter.Unmarshal(input, &rv)
		if err != nil {
			return nil, err
		}
		return &rv, nil
	}
	_, hasWildcard := tmp["wildcard"]
	if hasWildcard {
		var rv query.WildcardQuery
		err := jsoniter.Unmarshal(input, &rv)
		if err != nil {
			return nil, err
		}
		return &rv, nil
	}
	_, hasMatchAll := tmp["match_all"]
	if hasMatchAll {
		var rv query.MatchAllQuery
		err := jsoniter.Unmarshal(input, &rv)
		if err != nil {
			return nil, err
		}
		return &rv, nil
	}
	_, hasMatchNone := tmp["match_none"]
	if hasMatchNone {
		var rv query.MatchNoneQuery
		err := jsoniter.Unmarshal(input, &rv)
		if err != nil {
			return nil, err
		}
		return &rv, nil
	}
	_, hasDocIds := tmp["ids"]
	if hasDocIds {
		var rv query.DocIDQuery
		err := jsoniter.Unmarshal(input, &rv)
		if err != nil {
			return nil, err
		}
		return &rv, nil
	}
	_, hasBool := tmp["bool"]
	if hasBool {
		var rv query.BoolFieldQuery
		err := jsoniter.Unmarshal(input, &rv)
		if err != nil {
			return nil, err
		}
		return &rv, nil
	}
	_, hasTopLeft := tmp["top_left"]
	_, hasBottomRight := tmp["bottom_right"]
	if hasTopLeft && hasBottomRight {
		var rv query.GeoBoundingBoxQuery
		err := jsoniter.Unmarshal(input, &rv)
		if err != nil {
			return nil, err
		}
		return &rv, nil
	}
	_, hasDistance := tmp["distance"]
	if hasDistance {
		var rv query.GeoDistanceQuery
		err := jsoniter.Unmarshal(input, &rv)
		if err != nil {
			return nil, err
		}
		return &rv, nil
	}
	_, hasPoints := tmp["polygon_points"]
	if hasPoints {
		var rv query.GeoBoundingPolygonQuery
		err := jsoniter.Unmarshal(input, &rv)
		if err != nil {
			return nil, err
		}
		return &rv, nil
	}
	_, hasGeo := tmp["geometry"]
	if hasGeo {
		var rv query.GeoShapeQuery
		err := jsoniter.Unmarshal(input, &rv)
		if err != nil {
			return nil, err
		}
		return &rv, nil
	}
	_, hasCidr := tmp["cidr"]
	if hasCidr {
		var rv query.IPRangeQuery
		err := jsoniter.Unmarshal(input, &rv)
		if err != nil {
			return nil, err
		}
		return &rv, nil
	}
	return nil, fmt.Errorf("unknown query type")
}

// parsePreSearchData deserializes a JSON representation of
// a preSearchData object.
func parsePreSearchData(input []byte) (map[string]interface{}, error) {
	var rv map[string]interface{}

	var tmp map[string]json.RawMessage
	err := jsoniter.Unmarshal(input, &tmp)
	if err != nil {
		return nil, err
	}

	for k, v := range tmp {
		switch k {
		case search.KnnPreSearchDataKey:
			var value []*search.DocumentMatch
			if v != nil {
				err := jsoniter.Unmarshal(v, &value)
				if err != nil {
					return nil, err
				}
			}
			if rv == nil {
				rv = make(map[string]interface{})
			}
			rv[search.KnnPreSearchDataKey] = value
		}
	}
	return rv, nil
}
