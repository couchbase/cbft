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
	"testing"
)

func TestDocumentFilter(t *testing.T) {
	type testCase struct {
		docConfig     string
		documents     []string
		expectedTypes []string
		err           string
	}

	tests := []testCase{
		{
			docConfig: `{
				"mode": "custom",
				"doc_filter": {
					"typeA": {
						"start": "2019/01/01",
						"end": "2019/12/31",
						"datetime_parser": "dateFormat1",
						"order": 1,
						"field": "date"
					},
					"typeB": {
						"min": 87,
						"max": 99,
						"field": "number",
						"order": 2
					},
					"typeC": {
						"bool": true,
						"order": 3,
						"field": "boolean"
					},
					"typeD": {
					    "term": "lorem",
						"field": "term",
						"order": 4
					},
					"typeE": {
						"conjuncts": [
							{
							 	"start": "2024/01/01",
								"end": "2024/12/31",
								"datetime_parser": "dateFormat1",
								"field": "date"
							},
							{
								"min": 900,
								"max": 999,
								"field": "number"
							}
						],
						"order": 5
					},
					"typeF": {
						"conjuncts": [
							{
								"disjuncts": [
									{
										"start": "2024/01/01",
										"end": "2024/12/31",
										"datetime_parser": "dateFormat1",
										"field": "date"
									},
									{
										"min": 900,
										"max": 999,
										"field": "number"
									},
									{
										"term": "bleve",
										"field": "term"
									}
								],
								"min": 2
							},
							{
								"bool": false,
								"field": "boolean"
							}	
						],
						"order": 6
					}
				}
			}`,
			documents: []string{
				`{
					"date": "2019/09/02",
					"number": 88,
					"boolean": true,
					"term": "lorem"
				}`,
				`{
					"date": "2019-09-02T15:04:05Z",
					"number": 87,
					"boolean": true,
					"term": "dolor"
				}`,
				`{
					"date": "2023/09/02 12:32 PM",
					"number": 99,
					"boolean": true
				}`,
				`{
					"date": "2019/12/31 00:01 PM",
					"number": 222,
					"boolean": false
				}`,
				`{
					"number": 99,
					"boolean": true
				}`,
				`{
					"date": "2023-09-02 03:04:00"
				}`,
				`{
					"dummyField": "dummyValue"
				}`,
				`{
					"boolean": true
				}`,
				`{
					"date": "20 Aug 2019",
					"number": 88,
					"boolean": true
				}`,
				`{
					"date": "20 Aug 2024",
					"number": 980,
					"boolean": false,
					"term": "lorem"
				}`,
				`{
					"date": "20 Aug 2024",
					"number": 980,
					"boolean": false
				}`,
				`{
					"number": 980
				}`,
				`{
					"number": 980,
					"boolean": false
				}`,
				`{
					"number": 980,
					"boolean": false,
					"term": "bleve"
				}`,
			},
			expectedTypes: []string{
				"typeA",
				"typeB",
				"typeC",
				"_default",
				"typeC",
				"_default",
				"_default",
				"typeC",
				"typeA",
				"typeD",
				"typeE",
				"_default",
				"_default",
				"typeF",
			},
			err: "",
		},
		{
			docConfig: `{
				"mode": "custom",
				"doc_filter": {
					"typeA": {
						"start": "2019/01/01",
						"end": "2019/12/31"
					}
				}
			}`,
			documents: []string{
				`{
					"date": "2019/09/02"
				}`,
			},
			expectedTypes: []string{
				"_default",
			},
			err: "error validating document filter typeA: date range filter has invalid order or is not specified",
		},
		{
			docConfig: `{
				"mode": "custom",
				"doc_filter": {
					"typeA": {
						"start": "2019/01/01",
						"end": "2019/12/31",
						"order": 3
					}
				}
			}`,
			documents: []string{
				`{
					"date": "2019/09/02"
				}`,
			},
			expectedTypes: []string{
				"_default",
			},
			err: "error validating document filter typeA: date range filter must specify field",
		},
		{
			docConfig: `{
				"mode": "custom",
				"doc_filter": {
					"typeA": {
						"start": "2019/01/01",
						"end": "2019/12/31",
						"datetime_parser": "dateFormat2",
						"order": 3,
						"field": "slice.struct.slice.map.myDate"
					},
					"typeB": {
						"min": 87,
						"max": 99,
						"order": 2,
						"field": "slice.struct.slice.map.myNum"
					},
					"typeC": {
						"bool": true,
						"order": 1,
						"field": "slice.struct.slice.map.myBool"
					}
				}
			}`,
			documents: []string{
				`{
					"slice": [
						[
							{
								"struct": {
									"slice": [
										{
											"map": {
												"myBool": true,
												"myNum": 88,
												"myDate": "2019/09/02"
											}
										},
										{
											"map": {
												"myBool": false,
												"myNum": 87,
												"myDate": "2023/09/02"
											}
										},
										{
											"map": {
												"myBool": true,
												"myNum": 99,
												"myDate": "2023/09/02 12:32 PM"
											}
										}
									]
								}
							},
							{
								"struct": {
									"slice": [
										{
											"map": {
												"myBool": false,
												"myNum": 222,
												"myDate": "2019/12/31 00:01 PM"
											}
										},
										{
											"map": {
												"myBool": true,
												"myNum": 99,
												"myDate": "2023-09-02 03:04:00"
											}
										}
									]
								}
							}
						],
						[
							{
								"struct": {
									"slice": [
										{
											"map": {
												"myBool": true,
												"myNum": 88,
												"myDate": "20 Aug 2019"
											}
										},
										{
											"map": {
												"myBool": false,
												"myNum": 87,
												"myDate": "2019/08/20 12:32 PM"
											}
										},
										{
											"map": {
												"myBool": true,
												"myNum": 99,
												"myDate": "2023/09/02 12:32 PM"
											}
										}
									]
								}
							}
						]
					]
				}`,
				`{
					"slice": [
						[
							{
								"struct": {
									"slice": [
										{
											"map": {
												"myBool": false,
												"myNum": 88,
												"myDate": "2019/09/02"
											}
										},
										{
											"map": {
												"myBool": false,
												"myNum": 87,
												"myDate": "2023/09/02"
											}
										},
										{
											"map": {
												"myBool": false,
												"myNum": 99,
												"myDate": "2023/09/02 12:32 PM"
											}
										}
									]
								}
							},
							{
								"struct": {
									"slice": [
										{
											"map": {
												"myBool": false,
												"myNum": 222,
												"myDate": "2019/12/31 00:01 PM"
											}
										},
										{
											"map": {
												"myBool": false,
												"myNum": 99,
												"myDate": "2023-09-02 03:04:00"
											}
										}
									]
								}
							}
						],
						[
							{
								"struct": {
									"slice": [
										{
											"map": {
												"myBool": false,
												"myNum": 88,
												"myDate": "20 Aug 2019"
											}
										},
										{
											"map": {
												"myBool": false,
												"myNum": 87,
												"myDate": "2019/08/20 12:32 PM"
											}
										},
										{
											"map": {
												"myBool": false,
												"myNum": 99,
												"myDate": "2023/09/02 12:32 PM"
											}
										}
									]
								}
							}
						]
					]
				}`,
			},
			expectedTypes: []string{
				"typeC",
				"typeB",
			},
			err: "",
		},
		{
			docConfig: `{
				"mode": "custom",
				"doc_filter": {
					"typeA": {
						"start": "2019/01/01",
						"end": "2019/12/31",
						"min": 87,
						"order": 3
					}
				}
			}`,
			err: "error parsing document filter typeA: ambiguous document filter detected",
		},
		{
			docConfig: `{
				"mode": "custom",
				"doc_filter": {
					"typeA": {
						"bool": false,
						"term": "false",
						"field": "tester_bool",
						"order": 0
					}
				}
			}`,
			err: "error parsing document filter typeA: ambiguous document filter detected",
		},
		{
			docConfig: `{
				"mode": "custom",
				"doc_filter": {
					"typeA": {
						"conjuncts": [
							{
								"start": "2019/01/01",
								"end": "2019/12/31"
							},
							{
								"min": 87,
								"max": 99
							}
						],
						"order": 3,
						"disjuncts": [
							{
								"start": "2019/01/01",
								"end": "2019/12/31"
							},
							{
								"min": 87,
								"max": 99
							}
						]
					}
				}
			}`,
			err: "error parsing document filter typeA: ambiguous document filter detected",
		},
		{
			docConfig: `{
				"mode": "custom",
				"doc_filter": {
					"typeA": {
						"max": 233,
						"disjuncts": [
							{
								"start": "2019/01/01",
								"end": "2019/12/31"
							}
						],
						"order": 0
					}
				}
			}`,
			err: "error parsing document filter typeA: ambiguous document filter detected",
		},
		{
			docConfig: `{
				"mode": "custom",
				"doc_filter": {
					"typeA": {
						"max": 233,
						"min": 87,
						"disjuncts": [
							{
								"start": "2019/01/01",
								"end": "2019/12/31"
							}
						],
						"order": 0
					}
				}
			}`,
			err: "error parsing document filter typeA: ambiguous document filter detected",
		},
	}
	for _, test := range tests {
		params := getDummyParams(test.docConfig)
		abc := NewBleveParams()
		err := json.Unmarshal([]byte(params), &abc)
		if err != nil {
			if test.err == "" {
				t.Fatalf("unexpected error: %v", err)
			}
			if err.Error() != test.err {
				t.Fatalf("expected error: '%s', got '%s'", test.err, err.Error())
			}
			continue
		}
		err = abc.DocConfig.Validate(abc.Mapping)
		if err != nil {
			if test.err == "" {
				t.Fatalf("unexpected error: %v", err)
			}
			if err.Error() != test.err {
				t.Fatalf("expected error: '%s', got '%s'", test.err, err.Error())
			}
			continue
		}
		for i, document := range test.documents {
			var v interface{}
			err = json.Unmarshal([]byte(document), &v)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			typ := abc.DocConfig.DetermineType([]byte("anything"), v, "_default")
			if typ != test.expectedTypes[i] {
				t.Fatalf("expected type: '%s', got '%s'", test.expectedTypes[i], typ)
			}
		}
	}
}

func getDummyParams(doc_config string) string {
	rv :=
		`{
			"doc_config": ` + doc_config + `,
			"mapping": {
				"analysis": {
					"date_time_parsers": {
						"dateFormat1": {
							"layouts": [
								"2006/01/02",
								"02 Jan 2006",
								"2006/01/02 03:04 PM"
							],
							"type": "sanitizedgo"
						},
						"dateFormat2": {
							"layouts": [
								"yyyy/MM/dd",
								"dd MMM uuuu",
								"yyyy/MM/dd hh:mm a"
							],
							"type": "isostyle"
						}
					}
				},
				"default_analyzer": "standard",
				"default_datetime_parser": "dateTimeOptional",
				"default_field": "_all",
				"default_mapping": {
					"dynamic": true,
					"enabled": true
				},
				"default_type": "_default",
				"docvalues_dynamic": false,
				"index_dynamic": true,
				"store_dynamic": false,
				"type_field": "_type"
			},
			"store": {
				"indexType": "scorch",
				"segmentVersion": 15
			}
		}`
	return rv
}
