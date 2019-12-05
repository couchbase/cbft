//  Copyright (c) 2016 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the
//  License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an "AS
//  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
//  express or implied. See the License for the specific language
//  governing permissions and limitations under the License.

package cbft

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"strings"

	"github.com/couchbase/cbgt"
)

var ConfigModeColPrefix = "scope.collection"

var ConfigModeColPrefixLen = len(ConfigModeColPrefix)

type BleveInterface interface{}

type BleveDocument struct {
	typ            string
	BleveInterface `json:""`
}

func (c *BleveDocument) Type() string {
	return c.typ
}

func (c *BleveDocument) ExtendType(nTyp string) {
	c.typ = nTyp
}

type collMetaField struct {
	Typ      string
	Contents []byte
}

type BleveDocumentConfig struct {
	Mode             string         `json:"mode"`
	TypeField        string         `json:"type_field"`
	DocIDPrefixDelim string         `json:"docid_prefix_delim"`
	DocIDRegexp      *regexp.Regexp `json:"docid_regexp"`
	CollPrefixLookup map[uint32]collMetaField
	legacyMode       bool
}

func (b *BleveDocumentConfig) UnmarshalJSON(data []byte) error {
	docIDRegexp := ""
	if b.DocIDRegexp != nil {
		docIDRegexp = b.DocIDRegexp.String()
	}
	if b.CollPrefixLookup == nil {
		b.CollPrefixLookup = make(map[uint32]collMetaField, 1)
	}
	tmp := struct {
		Mode             string `json:"mode"`
		TypeField        string `json:"type_field"`
		DocIDPrefixDelim string `json:"docid_prefix_delim"`
		DocIDRegexp      string `json:"docid_regexp"`
		CollPrefixLookup map[uint32]collMetaField
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
		// TODO handle this aptly
		fallthrough
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
		Mode             string `json:"mode"`
		TypeField        string `json:"type_field"`
		DocIDPrefixDelim string `json:"docid_prefix_delim"`
		DocIDRegexp      string `json:"docid_regexp"`
	}{
		Mode:             b.Mode,
		TypeField:        b.TypeField,
		DocIDPrefixDelim: b.DocIDPrefixDelim,
		DocIDRegexp:      docIDRegexp,
	}
	return json.Marshal(&tmp)
}

func (b *BleveDocumentConfig) BuildDocumentEx(key, val []byte,
	defaultType string, extrasType cbgt.DestExtrasType,
	extras []byte) (*BleveDocument, error) {
	var v interface{}

	err := json.Unmarshal(val, &v)
	if err != nil {
		v = map[string]interface{}{}
	}

	bdoc := b.BuildDocumentFromObj(key, v, defaultType)
	if !b.legacyMode {
		// update the typ with `$scope.$collection.` prefix
		typ := bdoc.Type()
		cmf := b.CollPrefixLookup[binary.LittleEndian.Uint32(extras[4:])]
		typ = cmf.Typ + typ
		bdoc.ExtendType(typ)
	}
	return bdoc, err
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

func (b *BleveDocumentConfig) DetermineType(key []byte, v interface{}, defaultType string) string {
	i := strings.LastIndex(b.Mode, ConfigModeColPrefix)
	if i == -1 {
		i = 0
		b.legacyMode = true
	} else {
		if len(b.Mode) > ConfigModeColPrefixLen {
			i++
		}
		i += ConfigModeColPrefixLen
	}

	return b.findTypeUtil(key, v, defaultType, b.Mode[i:])
}

func (b *BleveDocumentConfig) findTypeUtil(key []byte, v interface{},
	defaultType, mode string) string {
	switch mode {
	case "type_field":
		typ, ok := mustString(lookupPropertyPath(v, b.TypeField))
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
	}
	return defaultType
}

func (b *BleveDocumentConfig) extendDocument(body []byte, fieldVal []byte) []byte {
	i := metaFieldPosition(body)
	if i == -1 { // no members found in json body
		i = 0
		if body[i] == bCurlyBraceStart {
			i++
		}

		fieldVal = fieldVal[1:] // skip `,` character in field bytes to embedd
	}
	return append(body[:i], fieldVal...)
}

// utility functions copied from bleve/reflect.go
func lookupPropertyPath(data interface{}, path string) interface{} {
	pathParts := decodePath(path)

	current := data
	for _, part := range pathParts {
		current = lookupPropertyPathPart(current, part)
		if current == nil {
			break
		}
	}

	return current
}

func lookupPropertyPathPart(data interface{}, part string) interface{} {
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
	}
	return nil
}

const pathSeparator = "."

func decodePath(path string) []string {
	return strings.Split(path, pathSeparator)
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
