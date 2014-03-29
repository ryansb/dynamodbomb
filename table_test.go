package ddbomb_test

import (
	"github.com/ryansb/dynamodbomb"
	"launchpad.net/gocheck"
)

type TableSuite struct {
	TableDescriptionT ddbomb.TableDescriptionT
	DynamoDBTest
}

func (s *TableSuite) SetUpSuite(c *gocheck.C) {
	setUpAuth(c)
	s.DynamoDBTest.TableDescriptionT = s.TableDescriptionT
	s.server = &ddbomb.Server{dynamodb_auth, dynamodb_region}
	pk, err := s.TableDescriptionT.BuildPrimaryKey()
	if err != nil {
		c.Skip(err.Error())
	}
	s.table = s.server.NewTable(s.TableDescriptionT.TableName, pk)

	// Cleanup
	s.TearDownSuite(c)
}

var table_suite = &TableSuite{
	TableDescriptionT: ddbomb.TableDescriptionT{
		TableName: "DynamoDBTestMyTable",
		AttributeDefinitions: []ddbomb.AttributeDefinitionT{
			ddbomb.AttributeDefinitionT{"TestHashKey", "S"},
			ddbomb.AttributeDefinitionT{"TestRangeKey", "N"},
		},
		KeySchema: []ddbomb.KeySchemaT{
			ddbomb.KeySchemaT{"TestHashKey", "HASH"},
			ddbomb.KeySchemaT{"TestRangeKey", "RANGE"},
		},
		ProvisionedThroughput: ddbomb.ProvisionedThroughputT{
			ReadCapacityUnits:  1,
			WriteCapacityUnits: 1,
		},
	},
}

var _ = gocheck.Suite(table_suite)

func (s *TableSuite) TestCreateListTable(c *gocheck.C) {
	status, err := s.server.CreateTable(s.TableDescriptionT)
	if err != nil {
		c.Fatal(err)
	}
	if status != "ACTIVE" && status != "CREATING" {
		c.Error("Expect status to be ACTIVE or CREATING")
	}

	s.WaitUntilStatus(c, "ACTIVE")

	tables, err := s.server.ListTables()
	if err != nil {
		c.Fatal(err)
	}
	c.Check(len(tables), gocheck.Not(gocheck.Equals), 0)
	c.Check(findTableByName(tables, s.TableDescriptionT.TableName), gocheck.Equals, true)
}
