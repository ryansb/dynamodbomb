package ddbomb

import (
	"encoding/json"
	"errors"
	"fmt"
	simplejson "github.com/bitly/go-simplejson"
)

type Table struct {
	Server *Server
	Name   string
	Key    PrimaryKey
}

type AttributeDefinitionT struct {
	Name string `json:"AttributeName"`
	Type string `json:"AttributeType"`
}

type KeySchemaT struct {
	AttributeName string
	KeyType       KeyType
}

type ProjectionT struct {
	ProjectionType string
}

type GlobalSecondaryIndexT struct {
	IndexName             string
	IndexSizeBytes        int64
	ItemCount             int64
	KeySchema             []KeySchemaT
	Projection            ProjectionT
	ProvisionedThroughput ProvisionedThroughputT
}

type LocalSecondaryIndexT struct {
	IndexName      string
	IndexSizeBytes int64
	ItemCount      int64
	KeySchema      []KeySchemaT
	Projection     ProjectionT
}

type ProvisionedThroughputT struct {
	NumberOfDecreasesToday int64
	ReadCapacityUnits      int64
	WriteCapacityUnits     int64
}

type TableDescriptionT struct {
	AttributeDefinitions   []AttributeDefinitionT
	CreationDateTime       float64
	ItemCount              int64
	KeySchema              []KeySchemaT
	LocalSecondaryIndexes  []LocalSecondaryIndexT
	GlobalSecondaryIndexes []GlobalSecondaryIndexT
	ProvisionedThroughput  ProvisionedThroughputT
	TableName              string
	TableSizeBytes         int64
	TableStatus            string
}

type describeTableResponse struct {
	Table TableDescriptionT
}

func findAttributeDefinitionByName(ads []AttributeDefinitionT, name string) *AttributeDefinitionT {
	for _, a := range ads {
		if a.Name == name {
			return &a
		}
	}
	return nil
}

func (a *AttributeDefinitionT) GetEmptyAttribute() *Attribute {
	switch a.Type {
	case "S":
		return NewStringAttribute(a.Name, "")
	case "N":
		return NewNumericAttribute(a.Name, "")
	case "B":
		return NewBinaryAttribute(a.Name, "")
	default:
		return nil
	}
}

func (t *TableDescriptionT) BuildPrimaryKey() (pk PrimaryKey, err error) {
	for _, k := range t.KeySchema {
		var attr *Attribute
		ad := findAttributeDefinitionByName(t.AttributeDefinitions, k.AttributeName)
		if ad == nil {
			return pk, errors.New("An inconsistency found in TableDescriptionT")
		}
		attr = ad.GetEmptyAttribute()
		if attr == nil {
			return pk, errors.New("An inconsistency found in TableDescriptionT")
		}

		switch k.KeyType {
		case HASH_KEY:
			pk.KeyAttribute = attr
		case RANGE_KEY:
			pk.RangeAttribute = attr
		}
	}
	return
}

func (s *Server) NewTable(name string, key PrimaryKey) *Table {
	return &Table{s, name, key}
}

func (s *Server) ListTables() ([]string, error) {
	var tables []string

	query := NewEmptyQuery()

	jsonResponse, err := s.queryServer(target("ListTables"), query)

	if err != nil {
		return nil, err
	}

	json, err := simplejson.NewJson(jsonResponse)

	if err != nil {
		return nil, err
	}

	response, err := json.Get("TableNames").Array()

	if err != nil {
		message := fmt.Sprintf("Unexpected response %s", jsonResponse)
		return nil, errors.New(message)
	}

	for _, value := range response {
		if t, ok := (value).(string); ok {
			tables = append(tables, t)
		}
	}

	return tables, nil
}

func (s *Server) CreateTable(tableDescription TableDescriptionT) (string, error) {
	query := NewEmptyQuery()
	query.AddCreateRequestTable(tableDescription)

	jsonResponse, err := s.queryServer(target("CreateTable"), query)

	if err != nil {
		return "unknown", err
	}

	json, err := simplejson.NewJson(jsonResponse)

	if err != nil {
		return "unknown", err
	}

	return json.Get("TableDescription").Get("TableStatus").MustString(), nil
}

func (s *Server) DeleteTable(tableDescription TableDescriptionT) (string, error) {
	query := NewEmptyQuery()
	query.AddDeleteRequestTable(tableDescription)

	jsonResponse, err := s.queryServer(target("DeleteTable"), query)

	if err != nil {
		return "unknown", err
	}

	json, err := simplejson.NewJson(jsonResponse)

	if err != nil {
		return "unknown", err
	}

	return json.Get("TableDescription").Get("TableStatus").MustString(), nil
}

func (t *Table) DescribeTable() (*TableDescriptionT, error) {
	return t.Server.DescribeTable(t.Name)
}

func (s *Server) DescribeTable(name string) (*TableDescriptionT, error) {
	q := NewEmptyQuery()
	q.addTableByName(name)

	jsonResponse, err := s.queryServer(target("DescribeTable"), q)
	if err != nil {
		return nil, err
	}

	var r describeTableResponse
	err = json.Unmarshal(jsonResponse, &r)
	if err != nil {
		return nil, err
	}

	return &r.Table, nil
}

func keyParam(k *PrimaryKey, hashKey string, rangeKey string) string {
	value := fmt.Sprintf("{\"HashKeyElement\":{%s}", keyValue(k.KeyAttribute.Type, hashKey))

	if k.RangeAttribute != nil {
		value = fmt.Sprintf("%s,\"RangeKeyElement\":{%s}", value,
			keyValue(k.RangeAttribute.Type, rangeKey))
	}

	return fmt.Sprintf("\"Key\":%s}", value)
}

func keyValue(key DataType, value string) string {
	return fmt.Sprintf("\"%v\":\"%s\"", key, value)
}
