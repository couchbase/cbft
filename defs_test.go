//  Copyright (c) 2014 Couchbase, Inc.
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
	"encoding/json"
	"testing"
)

func TestIndexDefs(t *testing.T) {
	d := NewIndexDefs("1.2.3")
	buf, _ := json.Marshal(d)
	d2 := &IndexDefs{}
	err := json.Unmarshal(buf, d2)
	if err != nil || d.UUID != d2.UUID || d.ImplVersion != d2.ImplVersion {
		t.Errorf("Unmarshal IndexDefs err or mismatch")
	}

	cfg := NewCfgMem()
	d3, cas, err := CfgGetIndexDefs(cfg)
	if err != nil || cas != 0 || d3 != nil {
		t.Errorf("CfgGetIndexDefs on new cfg should be nil")
	}
	cas, err = CfgSetIndexDefs(cfg, d, 100)
	if err == nil || cas != 0 {
		t.Errorf("expected error on CfgSetIndexDefs create on new cfg")
	}
	cas1, err := CfgSetIndexDefs(cfg, d, 0)
	if err != nil || cas1 != 1 {
		t.Errorf("expected ok on first save")
	}
	cas, err = CfgSetIndexDefs(cfg, d, 0)
	if err == nil || cas != 0 {
		t.Errorf("expected error on CfgSetIndexDefs recreate")
	}
	d4, cas, err := CfgGetIndexDefs(cfg)
	if err != nil || cas != cas1 ||
		d.UUID != d4.UUID || d.ImplVersion != d4.ImplVersion {
		t.Errorf("expected get to match first save")
	}
}

func TestNodeDefs(t *testing.T) {
	d := NewNodeDefs("1.2.3")
	buf, _ := json.Marshal(d)
	d2 := &NodeDefs{}
	err := json.Unmarshal(buf, d2)
	if err != nil || d.UUID != d2.UUID || d.ImplVersion != d2.ImplVersion {
		t.Errorf("UnmarshalNodeDefs err or mismatch")
	}

	cfg := NewCfgMem()
	d3, cas, err := CfgGetNodeDefs(cfg, NODE_DEFS_KNOWN)
	if err != nil || cas != 0 || d3 != nil {
		t.Errorf("CfgGetNodeDefs on new cfg should be nil")
	}
	cas, err = CfgSetNodeDefs(cfg, NODE_DEFS_KNOWN, d, 100)
	if err == nil || cas != 0 {
		t.Errorf("expected error on CfgSetNodeDefs create on new cfg")
	}
	cas1, err := CfgSetNodeDefs(cfg, NODE_DEFS_KNOWN, d, 0)
	if err != nil || cas1 != 1 {
		t.Errorf("expected ok on first save")
	}
	cas, err = CfgSetNodeDefs(cfg, NODE_DEFS_KNOWN, d, 0)
	if err == nil || cas != 0 {
		t.Errorf("expected error on CfgSetNodeDefs recreate")
	}
	d4, cas, err := CfgGetNodeDefs(cfg, NODE_DEFS_KNOWN)
	if err != nil || cas != cas1 ||
		d.UUID != d4.UUID || d.ImplVersion != d4.ImplVersion {
		t.Errorf("expected get to match first save")
	}
}

func TestPlanPIndexes(t *testing.T) {
	d := NewPlanPIndexes("1.2.3")
	buf, _ := json.Marshal(d)
	d2 := &PlanPIndexes{}
	err := json.Unmarshal(buf, d2)
	if err != nil || d.UUID != d2.UUID || d.ImplVersion != d2.ImplVersion {
		t.Errorf("UnmarshalPlanPIndexes err or mismatch")
	}

	cfg := NewCfgMem()
	d3, cas, err := CfgGetPlanPIndexes(cfg)
	if err != nil || cas != 0 || d3 != nil {
		t.Errorf("CfgGetPlanPIndexes on new cfg should be nil")
	}
	cas, err = CfgSetPlanPIndexes(cfg, d, 100)
	if err == nil || cas != 0 {
		t.Errorf("expected error on CfgSetPlanPIndexes create on new cfg")
	}
	cas1, err := CfgSetPlanPIndexes(cfg, d, 0)
	if err != nil || cas1 != 1 {
		t.Errorf("expected ok on first save")
	}
	cas, err = CfgSetPlanPIndexes(cfg, d, 0)
	if err == nil || cas != 0 {
		t.Errorf("expected error on CfgSetPlanPIndexes recreate")
	}
	d4, cas, err := CfgGetPlanPIndexes(cfg)
	if err != nil || cas != cas1 ||
		d.UUID != d4.UUID || d.ImplVersion != d4.ImplVersion {
		t.Errorf("expected get to match first save")
	}
}

func TestSamePlanPIndexes(t *testing.T) {
	a := NewPlanPIndexes("0.0.1")
	b := NewPlanPIndexes("0.0.1")
	c := NewPlanPIndexes("0.1.0")

	if !SamePlanPIndexes(nil, nil) {
		t.Errorf("expected same nil to nil")
	}
	if SamePlanPIndexes(a, nil) {
		t.Errorf("expected not same to nil")
	}
	if SamePlanPIndexes(nil, a) {
		t.Errorf("expected not same to nil")
	}
	if !SamePlanPIndexes(a, b) {
		t.Errorf("expected same, a: %v, b: %v", a, b)
	}
	if !SamePlanPIndexes(a, b) {
		t.Errorf("expected same, a: %v, b: %v", a, b)
	}
	if !SamePlanPIndexes(a, c) {
		t.Errorf("expected same, a: %v, c: %v", a, c)
	}
	if !SamePlanPIndexes(c, a) {
		t.Errorf("expected same, a: %v, c: %v", a, c)
	}

	a.PlanPIndexes["foo"] = &PlanPIndex{
		Name: "foo",
	}

	if SamePlanPIndexes(a, b) {
		t.Errorf("expected not same, a: %v, b: %v", a, b)
	}
	if SamePlanPIndexes(b, a) {
		t.Errorf("expected not same, a: %v, b: %v", a, b)
	}
	if SamePlanPIndexes(a, c) {
		t.Errorf("expected not same, a: %v, b: %v", a, b)
	}
	if SamePlanPIndexes(c, a) {
		t.Errorf("expected not same, a: %v, b: %v", a, b)
	}

	if SubsetPlanPIndexes(a, b) {
		t.Errorf("expected not same, a: %v, b: %v", a, b)
	}
	if !SubsetPlanPIndexes(b, a) {
		t.Errorf("expected not same, a: %v, b: %v", a, b)
	}

	b.PlanPIndexes["foo"] = &PlanPIndex{
		Name:      "foo",
		IndexName: "differnet-than-foo-in-a",
	}

	if SamePlanPIndexes(a, b) {
		t.Errorf("expected not same, a: %v, b: %v", a, b)
	}
	if SamePlanPIndexes(b, a) {
		t.Errorf("expected not same, a: %v, b: %v", a, b)
	}
}

func TestSamePlanPIndex(t *testing.T) {
	ppi0 := &PlanPIndex{
		Name:             "0",
		UUID:             "x",
		IndexName:        "x",
		IndexUUID:        "x",
		IndexParams:      "x",
		SourceType:       "x",
		SourceName:       "x",
		SourceUUID:       "x",
		SourcePartitions: "x",
		Nodes:            make(map[string]*PlanPIndexNode),
	}
	ppi1 := &PlanPIndex{
		Name:             "1",
		UUID:             "x",
		IndexName:        "x",
		IndexUUID:        "x",
		IndexParams:      "x",
		SourceType:       "x",
		SourceName:       "x",
		SourceUUID:       "x",
		SourcePartitions: "x",
		Nodes:            make(map[string]*PlanPIndexNode),
	}

	if !SamePlanPIndex(ppi0, ppi0) {
		t.Errorf("expected SamePlanPindex to be true")
	}
	if SamePlanPIndex(ppi0, ppi1) {
		t.Errorf("expected SamePlanPindex to be false")
	}
	if SamePlanPIndex(ppi1, ppi0) {
		t.Errorf("expected SamePlanPindex to be false")
	}
}

func TestPIndexMatchesPlan(t *testing.T) {
	plan := &PlanPIndex{
		Name: "hi",
		UUID: "111",
	}
	px := &PIndex{
		Name: "hi",
		UUID: "222",
	}
	py := &PIndex{
		Name: "hello",
		UUID: "111",
	}
	if PIndexMatchesPlan(px, plan) == false {
		t.Errorf("expected pindex to match the plan")
	}
	if PIndexMatchesPlan(py, plan) == true {
		t.Errorf("expected pindex to not match the plan")
	}
}

func TestCfgGetHelpers(t *testing.T) {
	errCfg := &ErrorOnlyCfg{}

	if _, err := CheckVersion(errCfg, "my-version"); err == nil {
		t.Errorf("expected to fail with errCfg")
	}
	if _, _, err := CfgGetIndexDefs(errCfg); err == nil {
		t.Errorf("expected to fail with errCfg")
	}
	if _, _, err := CfgGetNodeDefs(errCfg, NODE_DEFS_KNOWN); err == nil {
		t.Errorf("expected to fail with errCfg")
	}
	if _, _, err := CfgGetPlanPIndexes(errCfg); err == nil {
		t.Errorf("expected to fail with errCfg")
	}
}
