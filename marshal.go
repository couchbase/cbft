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
	"time"
	"unsafe"

	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/search"
	"github.com/blevesearch/bleve/v2/search/query"
	jsoniter "github.com/json-iterator/go"
)

func init() {
	// registers the custom json encoders with jsoniter
	registerCustomJSONEncoders()
}

// Marshal abstracts the underlying json lib used
func (p *CustomJSONImpl) Marshal(v interface{}) ([]byte, error) {
	return jsoniter.ConfigCompatibleWithStandardLibrary.Marshal(v)
}

// Encode abstracts the underlying json lib used
func (p *CustomJSONImpl) Encode(w io.Writer, v interface{}) error {
	return jsoniter.ConfigCompatibleWithStandardLibrary.NewEncoder(w).Encode(v)
}

// MarshalJSON abstracts the underlying json lib used
func MarshalJSON(v interface{}) ([]byte, error) {
	if JSONImpl != nil && JSONImpl.GetManagerOption("jsonImpl") != "std" {
		return JSONImpl.Marshal(v)
	}
	return json.Marshal(v)
}

func registerCustomJSONEncoders() {
	// adding all the custom encoders that bleve has implemented,
	// and need to extend as bleve introduces new custom encoders.
	jsoniter.RegisterTypeEncoderFunc("bleve.IndexErrMap", encodeBleveIndexErrMap, nil)

	jsoniter.RegisterTypeEncoderFunc("bleve.dateTimeRange", encodeBleveDateTimeRange, nil)

	jsoniter.RegisterTypeEncoderFunc("search.SortDocID", encodeSearchSortDocID, nil)

	jsoniter.RegisterTypeEncoderFunc("search.SortScore", encodeSearchSortScore, nil)

	jsoniter.RegisterTypeEncoderFunc("search.SortField", encodeSearchSortField, nil)

	jsoniter.RegisterTypeEncoderFunc("query.BleveQueryTime", encodeBleveQueryTime, nil)

	jsoniter.RegisterTypeEncoderFunc("search.SortGeoDistance", encodeSortGeoDistance, nil)

	jsoniter.RegisterTypeEncoderFunc("query.MatchAllQuery", encodeMatchAllQuery, nil)

	jsoniter.RegisterTypeEncoderFunc("query.MatchNoneQuery", encodeMatchNoneQuery, nil)

	jsoniter.RegisterTypeEncoderFunc("query.MatchQueryOperator", encodeMatchQueryOperator, nil)
}

func encodeBleveIndexErrMap(ptr unsafe.Pointer, stream *jsoniter.Stream) {
	mapPtr := unsafe.Pointer(&ptr)
	iem := *((*bleve.IndexErrMap)(mapPtr))
	tmp := make(map[string]string, len(iem))
	for k, v := range iem {
		tmp[k] = v.Error()
	}
	stream.WriteVal(tmp)
}

func encodeBleveDateTimeRange(ptr unsafe.Pointer, stream *jsoniter.Stream) {
	type temp struct {
		Name           string    `json:"name,omitempty"`
		Start          time.Time `json:"start,omitempty"`
		End            time.Time `json:"end,omitempty"`
		DateTimeParser string    `json:"datetime_parser,omitempty"`
		startString    *string
		endString      *string
	}
	dr := *((*temp)(ptr))

	rv := map[string]interface{}{
		"name": dr.Name,
	}

	if !dr.Start.IsZero() {
		rv["start"] = dr.Start
	} else if dr.startString != nil {
		rv["start"] = dr.startString
	}

	if !dr.End.IsZero() {
		rv["end"] = dr.End
	} else if dr.endString != nil {
		rv["end"] = dr.endString
	}

	if dr.DateTimeParser != "" {
		rv["datetime_parser"] = dr.DateTimeParser
	}

	stream.WriteVal(rv)
}

func encodeSearchSortDocID(ptr unsafe.Pointer, stream *jsoniter.Stream) {
	sid := *((*search.SortDocID)(ptr))
	if sid.Desc {
		stream.WriteString("-_id")
		return
	}
	stream.WriteString("_id")
}

func encodeSearchSortScore(ptr unsafe.Pointer, stream *jsoniter.Stream) {
	ss := *((*search.SortScore)(ptr))
	if ss.Desc {
		stream.WriteString("-_score")
		return
	}
	stream.WriteString("_score")
}

func encodeSearchSortField(ptr unsafe.Pointer, stream *jsoniter.Stream) {
	s := *((*search.SortField)(ptr))
	if s.Missing == search.SortFieldMissingLast &&
		s.Mode == search.SortFieldDefault &&
		s.Type == search.SortFieldAuto {
		if s.Desc {
			stream.WriteString("-" + s.Field)
			return

		}
		stream.WriteString(s.Field)
		return
	}
	sfm := map[string]interface{}{
		"by":    "field",
		"field": s.Field,
	}
	if s.Desc {
		sfm["desc"] = true
	}
	if s.Missing > search.SortFieldMissingLast {
		switch s.Missing {
		case search.SortFieldMissingFirst:
			sfm["missing"] = "first"
		}
	}
	if s.Mode > search.SortFieldDefault {
		switch s.Mode {
		case search.SortFieldMin:
			sfm["mode"] = "min"
		case search.SortFieldMax:
			sfm["mode"] = "max"
		}
	}
	if s.Type > search.SortFieldAuto {
		switch s.Type {
		case search.SortFieldAsString:
			sfm["type"] = "string"
		case search.SortFieldAsNumber:
			sfm["type"] = "number"
		case search.SortFieldAsDate:
			sfm["type"] = "date"
		}
	}
	stream.WriteVal(sfm)
}

func encodeBleveQueryTime(ptr unsafe.Pointer, stream *jsoniter.Stream) {
	temp := *((*query.BleveQueryTime)(ptr))
	tt := time.Time(temp.Time)
	stream.WriteString(tt.Format(query.QueryDateTimeFormat))
}

func encodeSortGeoDistance(ptr unsafe.Pointer, stream *jsoniter.Stream) {
	s := *((*search.SortGeoDistance)(ptr))
	sfm := map[string]interface{}{
		"by":    "geo_distance",
		"field": s.Field,
		"location": map[string]interface{}{
			"lon": s.Lon,
			"lat": s.Lat,
		},
	}
	if s.Unit != "" {
		sfm["unit"] = s.Unit
	}
	if s.Desc {
		sfm["desc"] = true
	}
	stream.WriteVal(sfm)
}

func encodeMatchAllQuery(ptr unsafe.Pointer, stream *jsoniter.Stream) {
	q := *((*query.MatchAllQuery)(ptr))
	tmp := map[string]interface{}{
		"boost":     q.BoostVal,
		"match_all": map[string]interface{}{},
	}
	stream.WriteVal(tmp)
}

func encodeMatchNoneQuery(ptr unsafe.Pointer, stream *jsoniter.Stream) {
	q := *((*query.MatchNoneQuery)(ptr))
	tmp := map[string]interface{}{
		"boost":      q.BoostVal,
		"match_none": map[string]interface{}{},
	}
	stream.WriteVal(tmp)
}

func encodeMatchQueryOperator(ptr unsafe.Pointer, stream *jsoniter.Stream) {
	o := *((*query.MatchQueryOperator)(ptr))
	switch o {
	case query.MatchQueryOperatorOr:
		stream.WriteString("or")
	case query.MatchQueryOperatorAnd:
		stream.WriteString("and")
	default:
		stream.Error = fmt.Errorf("cannot marshal match operator %d to JSON", o)
	}
}
