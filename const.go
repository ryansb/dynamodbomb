package ddbomb

type DataType string
type ComparisonType string
type ProjectionType string
type KeyType string

const (
	RANGE_KEY KeyType = "RANGE"
	HASH_KEY          = "HASH"

	PROJ_ALL       ProjectionType = "ALL"
	PROJ_KEYS_ONLY                = "KEYS_ONLY"
	PROJ_INCLUDE                  = "INCLUDE"

	STRING DataType = "S"
	NUMBER          = "N"
	BINARY          = "B"

	STRING_SET DataType = "SS"
	NUMBER_SET          = "NS"
	BINARY_SET          = "BS"

	CMP_EQUAL                    ComparisonType = "EQ"
	CMP_NOT_EQUAL                               = "NE"
	CMP_LESS_THAN_OR_EQUAL                      = "LE"
	CMP_LESS_THAN                               = "LT"
	CMP_GREATER_THAN_OR_EQUAL                   = "GE"
	CMP_GREATER_THAN                            = "GT"
	CMP_ATTRIBUTE_EXISTS                        = "NOT_NULL"
	CMP_ATTRIBUTE_DOES_NOT_EXIST                = "NULL"
	CMP_CONTAINS                                = "CONTAINS"
	CMP_DOES_NOT_CONTAIN                        = "NOT_CONTAINS"
	CMP_BEGINS_WITH                             = "BEGINS_WITH"
	CMP_IN                                      = "IN"
	CMP_BETWEEN                                 = "BETWEEN"
)
