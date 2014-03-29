package ddbomb_test

import (
	simplejson "github.com/bitly/go-simplejson"
	"github.com/crowdmob/goamz/aws"
	"github.com/ryansb/dynamodbomb"
	"launchpad.net/gocheck"
)

type QueryBuilderSuite struct {
	server *ddbomb.Server
}

var _ = gocheck.Suite(&QueryBuilderSuite{})

func (s *QueryBuilderSuite) SetUpSuite(c *gocheck.C) {
	auth := &aws.Auth{AccessKey: "", SecretKey: "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY"}
	s.server = &ddbomb.Server{*auth, aws.USEast}
}

func (s *QueryBuilderSuite) TestEmptyQuery(c *gocheck.C) {
	q := ddbomb.NewEmptyQuery()
	queryString := q.String()
	expectedString := "{}"
	c.Check(queryString, gocheck.Equals, expectedString)

	if expectedString != queryString {
		c.Fatalf("Unexpected Query String : %s\n", queryString)
	}
}

func (s *QueryBuilderSuite) TestAddWriteRequestItems(c *gocheck.C) {
	primary := ddbomb.NewStringAttribute("WidgetFoo", "")
	secondary := ddbomb.NewNumericAttribute("Created", "")
	key := ddbomb.PrimaryKey{primary, secondary}
	table := s.server.NewTable("FooData", key)

	primary2 := ddbomb.NewStringAttribute("TestHashKey", "")
	secondary2 := ddbomb.NewNumericAttribute("TestRangeKey", "")
	key2 := ddbomb.PrimaryKey{primary2, secondary2}
	table2 := s.server.NewTable("TestTable", key2)

	q := ddbomb.NewEmptyQuery()

	attribute1 := ddbomb.NewNumericAttribute("testing", "4")
	attribute2 := ddbomb.NewNumericAttribute("testingbatch", "2111")
	attribute3 := ddbomb.NewStringAttribute("testingstrbatch", "mystr")
	item1 := []ddbomb.Attribute{*attribute1, *attribute2, *attribute3}

	attribute4 := ddbomb.NewNumericAttribute("testing", "444")
	attribute5 := ddbomb.NewNumericAttribute("testingbatch", "93748249272")
	attribute6 := ddbomb.NewStringAttribute("testingstrbatch", "myotherstr")
	item2 := []ddbomb.Attribute{*attribute4, *attribute5, *attribute6}

	attributeDel1 := ddbomb.NewStringAttribute("TestHashKeyDel", "DelKey")
	attributeDel2 := ddbomb.NewNumericAttribute("TestRangeKeyDel", "7777777")
	itemDel := []ddbomb.Attribute{*attributeDel1, *attributeDel2}

	attributeTest1 := ddbomb.NewStringAttribute("TestHashKey", "MyKey")
	attributeTest2 := ddbomb.NewNumericAttribute("TestRangeKey", "0193820384293")
	itemTest := []ddbomb.Attribute{*attributeTest1, *attributeTest2}

	tableItems := map[*ddbomb.Table]map[string][][]ddbomb.Attribute{}
	actionItems := make(map[string][][]ddbomb.Attribute)
	actionItems["Put"] = [][]ddbomb.Attribute{item1, item2}
	actionItems["Delete"] = [][]ddbomb.Attribute{itemDel}
	tableItems[table] = actionItems

	actionItems2 := make(map[string][][]ddbomb.Attribute)
	actionItems2["Put"] = [][]ddbomb.Attribute{itemTest}
	tableItems[table2] = actionItems2

	q.AddWriteRequestItems(tableItems)

	queryJson, err := simplejson.NewJson([]byte(q.String()))
	if err != nil {
		c.Fatal(err)
	}

	expectedJson, err := simplejson.NewJson([]byte(`
{
  "RequestItems": {
    "TestTable": [
      {
        "PutRequest": {
          "Item": {
            "TestRangeKey": {
              "N": "0193820384293"
            },
            "TestHashKey": {
              "S": "MyKey"
            }
          }
        }
      }
    ],
    "FooData": [
      {
        "PutRequest": {
          "Item": {
            "testingstrbatch": {
              "S": "mystr"
            },
            "testingbatch": {
              "N": "2111"
            },
            "testing": {
              "N": "4"
            }
          }
        }
      },
      {
        "PutRequest": {
          "Item": {
            "testingstrbatch": {
              "S": "myotherstr"
            },
            "testingbatch": {
              "N": "93748249272"
            },
            "testing": {
              "N": "444"
            }
          }
        }
      },
      {
        "DeleteRequest": {
          "Key": {
            "TestRangeKeyDel": {
              "N": "7777777"
            },
            "TestHashKeyDel": {
              "S": "DelKey"
            }
          }
        }
      }
    ]
  }
}
	`))
	if err != nil {
		c.Fatal(err)
	}
	c.Check(queryJson, gocheck.DeepEquals, expectedJson)
}

func (s *QueryBuilderSuite) TestAddExpectedQuery(c *gocheck.C) {
	primary := ddbomb.NewStringAttribute("domain", "")
	key := ddbomb.PrimaryKey{primary, nil}
	table := s.server.NewTable("sites", key)

	q := ddbomb.NewQuery(table)
	q.AddKey(table, &ddbomb.Key{HashKey: "test"})

	expected := []ddbomb.Attribute{
		*ddbomb.NewStringAttribute("domain", "expectedTest").SetExists(true),
		*ddbomb.NewStringAttribute("testKey", "").SetExists(false),
	}
	q.AddExpected(expected)

	queryJson, err := simplejson.NewJson([]byte(q.String()))
	if err != nil {
		c.Fatal(err)
	}

	expectedJson, err := simplejson.NewJson([]byte(`
	{
		"Expected": {
			"domain": {
				"Exists": "true",
				"Value": {
					"S": "expectedTest"
				}
			},
			"testKey": {
				"Exists": "false"
			}
		},
		"Key": {
			"domain": {
				"S": "test"
			}
		},
		"TableName": "sites"
	}
	`))
	if err != nil {
		c.Fatal(err)
	}
	c.Check(queryJson, gocheck.DeepEquals, expectedJson)
}

func (s *QueryBuilderSuite) TestGetItemQuery(c *gocheck.C) {
	primary := ddbomb.NewStringAttribute("domain", "")
	key := ddbomb.PrimaryKey{primary, nil}
	table := s.server.NewTable("sites", key)

	q := ddbomb.NewQuery(table)
	q.AddKey(table, &ddbomb.Key{HashKey: "test"})

	{
		queryJson, err := simplejson.NewJson([]byte(q.String()))
		if err != nil {
			c.Fatal(err)
		}

		expectedJson, err := simplejson.NewJson([]byte(`
		{
			"Key": {
				"domain": {
					"S": "test"
				}
			},
			"TableName": "sites"
		}
		`))
		if err != nil {
			c.Fatal(err)
		}
		c.Check(queryJson, gocheck.DeepEquals, expectedJson)
	}

	// Use ConsistentRead
	{
		q.ConsistentRead(true)
		queryJson, err := simplejson.NewJson([]byte(q.String()))
		if err != nil {
			c.Fatal(err)
		}

		expectedJson, err := simplejson.NewJson([]byte(`
		{
			"ConsistentRead": "true",
			"Key": {
				"domain": {
					"S": "test"
				}
			},
			"TableName": "sites"
		}
		`))
		if err != nil {
			c.Fatal(err)
		}
		c.Check(queryJson, gocheck.DeepEquals, expectedJson)
	}
}

func (s *QueryBuilderSuite) TestUpdateQuery(c *gocheck.C) {
	primary := ddbomb.NewStringAttribute("domain", "")
	rangek := ddbomb.NewNumericAttribute("time", "")
	key := ddbomb.PrimaryKey{primary, rangek}
	table := s.server.NewTable("sites", key)

	countAttribute := ddbomb.NewNumericAttribute("count", "4")
	attributes := []ddbomb.Attribute{*countAttribute}

	q := ddbomb.NewQuery(table)
	q.AddKey(table, &ddbomb.Key{HashKey: "test", RangeKey: "1234"})
	q.AddUpdates(attributes, "ADD")

	queryJson, err := simplejson.NewJson([]byte(q.String()))
	if err != nil {
		c.Fatal(err)
	}
	expectedJson, err := simplejson.NewJson([]byte(`
{
	"AttributeUpdates": {
		"count": {
			"Action": "ADD",
			"Value": {
				"N": "4"
			}
		}
	},
	"Key": {
		"domain": {
			"S": "test"
		},
		"time": {
			"N": "1234"
		}
	},
	"TableName": "sites"
}
	`))
	if err != nil {
		c.Fatal(err)
	}
	c.Check(queryJson, gocheck.DeepEquals, expectedJson)
}

func (s *QueryBuilderSuite) TestAddUpdates(c *gocheck.C) {
	primary := ddbomb.NewStringAttribute("domain", "")
	key := ddbomb.PrimaryKey{primary, nil}
	table := s.server.NewTable("sites", key)

	q := ddbomb.NewQuery(table)
	q.AddKey(table, &ddbomb.Key{HashKey: "test"})

	attr := ddbomb.NewStringSetAttribute("StringSet", []string{"str", "str2"})

	q.AddUpdates([]ddbomb.Attribute{*attr}, "ADD")

	queryJson, err := simplejson.NewJson([]byte(q.String()))
	if err != nil {
		c.Fatal(err)
	}
	expectedJson, err := simplejson.NewJson([]byte(`
{
	"AttributeUpdates": {
		"StringSet": {
			"Action": "ADD",
			"Value": {
				"SS": ["str", "str2"]
			}
		}
	},
	"Key": {
		"domain": {
			"S": "test"
		}
	},
	"TableName": "sites"
}
	`))
	if err != nil {
		c.Fatal(err)
	}
	c.Check(queryJson, gocheck.DeepEquals, expectedJson)
}

func (s *QueryBuilderSuite) TestAddKeyConditions(c *gocheck.C) {
	primary := ddbomb.NewStringAttribute("domain", "")
	key := ddbomb.PrimaryKey{primary, nil}
	table := s.server.NewTable("sites", key)

	q := ddbomb.NewQuery(table)
	acs := []ddbomb.AttributeComparison{
		*ddbomb.NewStringAttributeComparison("domain", "EQ", "example.com"),
		*ddbomb.NewStringAttributeComparison("path", "EQ", "/"),
	}
	q.AddKeyConditions(acs)
	queryJson, err := simplejson.NewJson([]byte(q.String()))

	if err != nil {
		c.Fatal(err)
	}

	expectedJson, err := simplejson.NewJson([]byte(`
{
  "KeyConditions": {
    "domain": {
      "AttributeValueList": [
        {
          "S": "example.com"
        }
      ],
      "ComparisonOperator": "EQ"
    },
    "path": {
      "AttributeValueList": [
        {
          "S": "/"
        }
      ],
      "ComparisonOperator": "EQ"
    }
  },
  "TableName": "sites"
}
	`))
	if err != nil {
		c.Fatal(err)
	}
	c.Check(queryJson, gocheck.DeepEquals, expectedJson)
}
