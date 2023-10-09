//  Copyright 2016-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package cbft

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"sort"
	"strings"

	"github.com/blevesearch/bleve/v2/mapping"
	"github.com/couchbase/cbgt"
	log "github.com/couchbase/clog"
)

var ConfigModeCollPrefix = "scope.collection"
var ConfigModeCollPrefixLen = len(ConfigModeCollPrefix)

var CollMetaFieldName = "_$scope_$collection"

var reservedXattrsKeys = map[string]struct{}{
	"txn": struct{}{},
}

type BleveInterface interface{}

type BleveDocument struct {
	typ            string
	BleveInterface `json:""`
}

func (c *BleveDocument) Type() string {
	return c.typ
}

func (c *BleveDocument) SetType(nTyp string) {
	c.typ = nTyp
}

type collMetaField struct {
	scopeDotColl string
	typeMappings []string // for multiple mappings for a collection
	value        string   // _$<scope_id>_$<collection_id>
}

type typeForFilter struct {
	typ    string
	filter IndexFilter
}

type BleveDocumentConfig struct {
	Mode             string                 `json:"mode"`
	TypeField        string                 `json:"type_field"`
	DocIDPrefixDelim string                 `json:"docid_prefix_delim"`
	DocIDRegexp      *regexp.Regexp         `json:"docid_regexp"`
	DocumentFilter   map[string]IndexFilter `json:"doc_filter"`
	sortedFilters    []*typeForFilter
	CollPrefixLookup map[uint32]*collMetaField
	legacyMode       bool
}

func (bd *BleveDocumentConfig) Validate(idxMapping mapping.IndexMapping) error {
	if bd.DocumentFilter != nil {
		for _, filter := range bd.DocumentFilter {
			err := filter.Validate(idxMapping, true, 0)
			if err != nil {
				return err
			}
		}
		keys := make([]string, 0, len(bd.DocumentFilter))
		for key := range bd.DocumentFilter {
			keys = append(keys, key)
		}

		// ensure that none of the filters have the same order
		// as the order is used to sort the filters
		seenOrders := make(map[int]struct{})
		for _, key := range keys {
			order := bd.DocumentFilter[key].GetOrder()
			if _, ok := seenOrders[order]; ok {
				return fmt.Errorf("filter %s has the same order as another filter", key)
			}
			seenOrders[order] = struct{}{}
		}

		// Sort the keys based on the filter's order
		sort.SliceStable(keys, func(i, j int) bool {
			return bd.DocumentFilter[keys[i]].GetOrder() < bd.DocumentFilter[keys[j]].GetOrder()
		})

		bd.sortedFilters = make([]*typeForFilter, len(keys))
		for i, key := range keys {
			bd.sortedFilters[i] = &typeForFilter{
				typ:    key,
				filter: bd.DocumentFilter[key],
			}
		}
	}
	return nil
}
func (b *BleveDocumentConfig) UnmarshalJSON(data []byte) error {
	docIDRegexp := ""
	if b.DocIDRegexp != nil {
		docIDRegexp = b.DocIDRegexp.String()
	}
	if b.CollPrefixLookup == nil {
		b.CollPrefixLookup = make(map[uint32]*collMetaField, 1)
	}
	tmp := struct {
		Mode             string                     `json:"mode"`
		TypeField        string                     `json:"type_field"`
		DocIDPrefixDelim string                     `json:"docid_prefix_delim"`
		DocIDRegexp      string                     `json:"docid_regexp"`
		DocumentFilter   map[string]json.RawMessage `json:"doc_filter"`
		CollPrefixLookup map[uint32]*collMetaField
	}{
		Mode:             b.Mode,
		TypeField:        b.TypeField,
		DocIDPrefixDelim: b.DocIDPrefixDelim,
		DocIDRegexp:      docIDRegexp,
		CollPrefixLookup: b.CollPrefixLookup,
	}
	err := json.Unmarshal(data, &tmp)
	if err != nil {
		return err
	}
	b.Mode = tmp.Mode
	switch tmp.Mode {
	case "scope.collection":
		return nil
	case "scope.collection.type_field":
		fallthrough
	case "type_field":
		b.TypeField = tmp.TypeField
		if b.TypeField == "" {
			return fmt.Errorf("with mode type_field, type_field cannot be empty")
		}
	case "scope.collection.docid_prefix":
		fallthrough
	case "docid_prefix":
		b.DocIDPrefixDelim = tmp.DocIDPrefixDelim
		if b.DocIDPrefixDelim == "" {
			return fmt.Errorf("with mode docid_prefix, docid_prefix_delim cannot be empty")
		}
	case "scope.collection.docid_regexp":
		fallthrough
	case "docid_regexp":
		if tmp.DocIDRegexp != "" {
			b.DocIDRegexp, err = regexp.Compile(tmp.DocIDRegexp)
			if err != nil {
				return err
			}
		} else {
			return fmt.Errorf("with mode docid_regexp, docid_regexp cannot be empty")
		}
	case "scope.collection.custom":
		fallthrough
	case "custom":
		if tmp.DocumentFilter != nil {
			b.DocumentFilter = make(map[string]IndexFilter, len(tmp.DocumentFilter))
			for k, v := range tmp.DocumentFilter {
				b.DocumentFilter[k], err = ParseDocumentFilter(v)
				if err != nil {
					return err
				}
			}
		} else {
			return fmt.Errorf("with mode custom, doc_filter cannot be empty")
		}
	default:
		return fmt.Errorf("unknown mode: %s", tmp.Mode)
	}

	return nil
}

func (b *BleveDocumentConfig) MarshalJSON() ([]byte, error) {
	docIDRegexp := ""
	if b.DocIDRegexp != nil {
		docIDRegexp = b.DocIDRegexp.String()
	}
	tmp := struct {
		Mode             string                     `json:"mode"`
		TypeField        string                     `json:"type_field"`
		DocIDPrefixDelim string                     `json:"docid_prefix_delim"`
		DocIDRegexp      string                     `json:"docid_regexp"`
		DocumentFilter   map[string]json.RawMessage `json:"doc_filter,omitempty"`
	}{
		Mode:             b.Mode,
		TypeField:        b.TypeField,
		DocIDPrefixDelim: b.DocIDPrefixDelim,
		DocIDRegexp:      docIDRegexp,
	}
	tmp.DocumentFilter = make(map[string]json.RawMessage, len(b.DocumentFilter))
	for k, v := range b.DocumentFilter {
		jsonBytes, err := json.Marshal(v)
		if err != nil {
			return nil, err
		}
		tmp.DocumentFilter[k] = jsonBytes
	}
	return json.Marshal(&tmp)
}

func (b *BleveDocumentConfig) multiCollection() bool {
	return len(b.CollPrefixLookup) > 1
}

func (b *BleveDocumentConfig) BuildDocumentEx(key, val []byte,
	defaultType string, extrasType cbgt.DestExtrasType,
	req interface{}, extras []byte) (*BleveDocument, []byte, error) {

	var cmf *collMetaField
	var xattrs map[string]interface{}
	var collectionId []byte
	var err error

	// extract the metadata associated with the DCP mutation operation only if
	// the ingestion is via gocbcore for eg the extras might have xAttrs or other
	// metadata info useful while building the document to be indexed.
	//
	// in case of non-gocbcore ingestion, the extras are to be extracted according
	// whatever interface is dictated which is inline with the extrasType.
	// for eg in case of feed type gocouchbase, extra type is DEST_EXTRAS_TYPE_MCREQUEST and
	// the req interface{} is of type *gomemcached.MCRequest
	if extrasType == cbgt.DEST_EXTRAS_TYPE_GOCBCORE_DCP {
		gocbcoreExtras, ok := req.(cbgt.GocbcoreDCPExtras)
		if !ok {
			return nil, nil, fmt.Errorf("bleve: DataUpdateEx unable to typecast GocbcoreDCPExtras")
		}

		if gocbcoreExtras.Datatype&4 > 0 {
			cmf = b.CollPrefixLookup[gocbcoreExtras.CollectionId]
			xattrs, val, err = b.buildXAttrs(val)
			if err != nil {
				log.Errorf("BuildDocumentEx: error parsing xattrs: %v", err)
			}
			collectionId = make([]byte, 4)
			binary.LittleEndian.PutUint32(collectionId[0:], gocbcoreExtras.CollectionId)
		} else {
			cmf = b.CollPrefixLookup[gocbcoreExtras.CollectionId]
			collectionId = make([]byte, 4)
			binary.LittleEndian.PutUint32(collectionId, gocbcoreExtras.CollectionId)
		}
	}

	if len(extras) >= 8 {
		cmf = b.CollPrefixLookup[binary.LittleEndian.Uint32(extras[4:])]
		collectionId = extras[4:8]
	}

	var v map[string]interface{}
	err = json.Unmarshal(val, &v)
	if err != nil || v == nil {
		v = map[string]interface{}{}
	}

	// Add the xattr fields back into the document mapping
	// under the xattrs field mapping
	if _, ok := v[xattrsMappingName]; !ok && xattrs != nil {
		v[xattrsMappingName] = xattrs
	}

	if cmf != nil && len(b.CollPrefixLookup) > 1 {
		// more than 1 collection indexed
		key = append(collectionId, key...)
		v[CollMetaFieldName] = cmf.value
	}

	bdoc := b.BuildDocumentFromObj(key, v, defaultType)
	if !b.legacyMode && cmf != nil {
		typ := cmf.scopeDotColl
		// there could be multiple type mappings under a single collection.
		for _, t := range cmf.typeMappings {
			if len(t) > 0 && bdoc.Type() == t {
				// append type information only if the type mapping specifies a
				// 'type' and the document's matches it.
				typ += "." + t
				break
			}
		}
		bdoc.SetType(typ)
	}

	return bdoc, key, err
}

// BuildDocument returns a BleveDocument for the k/v pair
// NOTE: err may be non-nil AND a document is returned
// this allows the error to be logged, but a stub document to be indexed
func (b *BleveDocumentConfig) BuildDocument(key, val []byte,
	defaultType string) (*BleveDocument, error) {
	var v interface{}
	err := json.Unmarshal(val, &v)
	if err != nil {
		v = map[string]interface{}{}
	}

	return b.BuildDocumentFromObj(key, v, defaultType), err
}

func (b *BleveDocumentConfig) BuildDocumentFromObj(key []byte, v interface{},
	defaultType string) *BleveDocument {
	return &BleveDocument{
		typ:            b.DetermineType(key, v, defaultType),
		BleveInterface: v,
	}
}

func (b *BleveDocumentConfig) buildXAttrs(val []byte) (
	map[string]interface{}, []byte, error) {

	xattrs := make(map[string]interface{})

	if len(val) < 4 {
		return nil, val, fmt.Errorf("buildXAttrs: length of mutation less than" +
			"the required amount")
	}

	pos := uint32(0)
	// First 4 bytes determines the size of the xattr content
	xattrLen := binary.BigEndian.Uint32(val[pos : pos+4])
	pos = pos + 4

	if xattrLen == 0 {
		val = val[pos+4:]
	} else {
		var valInterface interface{}

		// Each xattr key and value pair is separated by \x00
		separator := []byte("\x00")

		for pos < xattrLen {
			// The next 4 bytes determines the size of
			// the key value pair
			pairLen := binary.BigEndian.Uint32(val[pos : pos+4])
			if pairLen == 0 || int(pos+pairLen+4) > len(val) {
				return nil, val, fmt.Errorf("buildXAttrs: length of mutation less than" +
					"the required amount")
			}
			pos += 4
			pairBytes := val[pos : pos+pairLen]
			// The key value looks like [key]\x00[value]\x00
			components := bytes.Split(pairBytes, separator)

			if len(components) != 3 {
				return nil, val, fmt.Errorf("buildXAttrs: wrong number of separators in xattrs")
			}

			xattrKey := string(components[0])

			// Exclude system xattrs and any reserved xattr keys from being
			// added into the document
			if _, ok := reservedXattrsKeys[xattrKey]; !ok && xattrKey[0] != '_' {
				// Check and parse the value into a string or a json
				if err := json.Unmarshal(components[1], &valInterface); err == nil {
					xattrs[xattrKey] = valInterface
				} else {
					xattrs[xattrKey] = string(components[1])
				}
			}

			pos += pairLen
		}
		val = val[pos:]
	}

	return xattrs, val, nil
}

func (b *BleveDocumentConfig) DetermineType(key []byte, v interface{}, defaultType string) string {
	i := strings.LastIndex(b.Mode, ConfigModeCollPrefix)
	if i == -1 {
		i = 0
		b.legacyMode = true
	} else {
		i += ConfigModeCollPrefixLen
		if len(b.Mode) > ConfigModeCollPrefixLen {
			// consider the "." that follows scope.collection
			i++
		}
	}

	return b.findTypeUtil(key, v, defaultType, b.Mode[i:])
}

func (b *BleveDocumentConfig) findTypeUtil(key []byte, v interface{},
	defaultType, mode string) string {
	switch mode {
	case "type_field":
		typ, ok := mustString(lookupPropertyPath(v, b.TypeField, false))
		if ok {
			return typ
		}
	case "docid_prefix":
		index := bytes.Index(key, []byte(b.DocIDPrefixDelim))
		if index > 0 {
			return string(key[0:index])
		}
	case "docid_regexp":
		typ := b.DocIDRegexp.Find(key)
		if typ != nil {
			return string(typ)
		}
	case "custom":
		for _, docFilter := range b.sortedFilters {
			if docFilter.filter.IsSatisfied(v) {
				return docFilter.typ
			}
		}
	}
	return defaultType
}

// utility functions copied from bleve/reflect.go
func lookupPropertyPath(data interface{}, path string, checkSlice bool) interface{} {
	pathParts := decodePath(path)

	current := data
	for _, part := range pathParts {
		current = lookupPropertyPathPart(current, part, checkSlice)
		if current == nil {
			break
		}
	}

	return current
}

func lookupPropertyPathPart(data interface{}, part string, checkSlice bool) interface{} {
	val := reflect.ValueOf(data)
	if !val.IsValid() {
		return nil
	}
	typ := val.Type()
	switch typ.Kind() {
	case reflect.Map:
		// TODO can add support for other map keys in the future
		if typ.Key().Kind() == reflect.String {
			if key := reflect.ValueOf(part); key.IsValid() {
				entry := val.MapIndex(key)
				if entry.IsValid() {
					return entry.Interface()
				}
			}
		}
	case reflect.Struct:
		field := val.FieldByName(part)
		if field.IsValid() && field.CanInterface() {
			return field.Interface()
		}
	case reflect.Slice:
		if checkSlice {
			rv := make([]interface{}, 0, val.Len())
			for i := 0; i < val.Len(); i++ {
				if val.Index(i).CanInterface() {
					fieldVal := val.Index(i).Interface()
					rv = append(rv, lookupPropertyPathPart(fieldVal, part, checkSlice))
				}
			}
			return flattenNestedSlice(rv)
		}
	}
	return nil
}

func flattenNestedSlice(slice []interface{}) []interface{} {
	var result []interface{}
	for _, item := range slice {
		switch item := item.(type) {
		case []interface{}:
			flattened := flattenNestedSlice(item)
			result = append(result, flattened...)
		case interface{}:
			result = append(result, item)
		}
	}
	return result
}

const pathSeparator = "."

func decodePath(path string) []string {
	return strings.Split(path, pathSeparator)
}
