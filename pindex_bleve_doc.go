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
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"strings"
)

type BleveInterface interface{}

type BleveDocument struct {
	typ            string
	BleveInterface `json:""`
}

func (c *BleveDocument) Type() string {
	return c.typ
}

type BleveDocumentConfig struct {
	Mode             string         `json:"mode"`
	TypeField        string         `json:"type_field"`
	DocIDPrefixDelim string         `json:"docid_prefix_delim"`
	DocIDRegexp      *regexp.Regexp `json:"docid_regexp"`
}

func (b *BleveDocumentConfig) UnmarshalJSON(data []byte) error {
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
	err := json.Unmarshal(data, &tmp)
	if err != nil {
		return err
	}
	b.Mode = tmp.Mode
	switch tmp.Mode {
	case "type_field":
		b.TypeField = tmp.TypeField
		if b.TypeField == "" {
			return fmt.Errorf("with mode type_field, type_field cannot be empty")
		}
	case "docid_prefix":
		b.DocIDPrefixDelim = tmp.DocIDPrefixDelim
		if b.DocIDPrefixDelim == "" {
			return fmt.Errorf("with mode docid_prefix, docid_prefix_delim cannot be empty")
		}
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

// BuildDocument returns a CbftDocument for the k/v pair
// NOTE: err may be non-nil AND a document is returned
// this allows the error to be logged, but a stub document to be indexed
func (b *BleveDocumentConfig) BuildDocument(key, val []byte, defaultType string) (*BleveDocument, error) {
	var v interface{}

	err := json.Unmarshal(val, &v)
	if err != nil {
		v = map[string]interface{}{}
	}

	typ := b.determineType(key, v, defaultType)

	doc := BleveDocument{
		typ:            typ,
		BleveInterface: v,
	}

	return &doc, err
}

func (b *BleveDocumentConfig) determineType(key []byte, v interface{}, defaultType string) string {
	switch b.Mode {
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
		// FIXME can add support for other map keys in the future
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
