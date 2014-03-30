package ddbomb

import (
	"reflect"
	"strconv"
)

type Key struct {
	HashKey  string
	RangeKey string
}

type PrimaryKey struct {
	KeyAttribute   *Attribute
	RangeAttribute *Attribute
}

type Attribute struct {
	Type      DataType
	Name      string
	Value     string
	SetValues []string
	Exists    string // exists on dynamodb? Values: "true", "false", or ""
}

type AttributeComparison struct {
	AttributeName      string
	ComparisonOperator ComparisonType
	AttributeValueList []Attribute // contains attributes with only types and names (value ignored)
}

func NewEqualInt64AttributeComparison(attributeName string, equalToValue int64) *AttributeComparison {
	return newComparison(attributeName, NUMBER, CMP_EQUAL, strconv.FormatInt(equalToValue, 10))
}

func NewEqualStringAttributeComparison(attributeName string, equalToValue string) *AttributeComparison {
	return newComparison(attributeName, STRING, CMP_EQUAL, equalToValue)
}

func NewStringAttributeComparison(attributeName string, comparisonOperator ComparisonType, value string) *AttributeComparison {
	return newComparison(attributeName, STRING, comparisonOperator, value)
}

func NewNumericAttributeComparison(attributeName string, comparisonOperator ComparisonType, value int64) *AttributeComparison {
	return newComparison(attributeName, NUMBER, comparisonOperator, strconv.FormatInt(value, 10))
}

func NewBinaryAttributeComparison(attributeName string, comparisonOperator ComparisonType, value bool) *AttributeComparison {
	return newComparison(attributeName, BINARY, comparisonOperator, strconv.FormatBool(value))
}

func NewStringAttribute(name string, value string) *Attribute {
	return &Attribute{
		Type:  STRING,
		Name:  name,
		Value: value,
	}
}

func NewNumericAttribute(name string, value string) *Attribute {
	return &Attribute{
		Type:  NUMBER,
		Name:  name,
		Value: value,
	}
}

func NewBinaryAttribute(name string, value string) *Attribute {
	return &Attribute{
		Type:  BINARY,
		Name:  name,
		Value: value,
	}
}

func NewStringSetAttribute(name string, values []string) *Attribute {
	return &Attribute{
		Type:      STRING_SET,
		Name:      name,
		SetValues: values,
	}
}

func NewNumericSetAttribute(name string, values []string) *Attribute {
	return &Attribute{
		Type:      NUMBER_SET,
		Name:      name,
		SetValues: values,
	}
}

func NewBinarySetAttribute(name string, values []string) *Attribute {
	return &Attribute{
		Type:      BINARY_SET,
		Name:      name,
		SetValues: values,
	}
}

func (a *Attribute) SetType() bool {
	switch a.Type {
	case BINARY_SET, NUMBER_SET, STRING_SET:
		return true
	}
	return false
}

func (a *Attribute) SetExists(exists bool) *Attribute {
	if exists {
		a.Exists = "true"
	} else {
		a.Exists = "false"
	}
	return a
}

func (k *PrimaryKey) HasRange() bool {
	return k.RangeAttribute != nil
}

// Useful when you may have many goroutines using a primary key, so they don't fuxor up your values.
func (k *PrimaryKey) Clone(h string, r string) []Attribute {
	pk := &Attribute{
		Type:  k.KeyAttribute.Type,
		Name:  k.KeyAttribute.Name,
		Value: h,
	}

	result := []Attribute{*pk}

	if k.HasRange() {
		rk := &Attribute{
			Type:  k.RangeAttribute.Type,
			Name:  k.RangeAttribute.Name,
			Value: r,
		}

		result = append(result, *rk)
	}

	return result
}

func newComparison(attributeName string, dataType DataType, comparisonOperator ComparisonType, value ...string) *AttributeComparison {
	attrs := make([]Attribute, 1)
	if dataType == NUMBER || dataType == STRING || dataType == BINARY {
		attrs[0] = Attribute{
			Type:  dataType,
			Name:  attributeName,
			Value: value[0],
		}
	} else {
		attrs[0] = Attribute{
			Type:      dataType,
			Name:      attributeName,
			SetValues: value,
		}
	}
	return &AttributeComparison{attributeName,
		comparisonOperator,
		attrs,
	}
}

/* Takes any struct and prepares it to save to dynamodb. To make sure a field
 * never gets saved in dynamodb, use the struct tag "ddbignore"
 * type MyThing struct {
 *     unHashedPassword string `ddbignore:"please"` // will never be stored in dynamo
 *     HashedPassword   string `ddbname:"hashpw"`   // change the name of the attribute in dynamodb
 *     Thumbnail        []byte `ddbtype:"BINARY"`   // make sure it's saved as binary
 * }
 */
func fromArbitrary(in interface{}) (attrs []Attribute) {
	st := reflect.TypeOf(in)
	for i := 0; i < st.NumField(); i++ {
		field := st.Field(i)
		if field.Tag.Get("ddbignore") != "" {
			continue
		}
		fName := field.Tag.Get("ddbname")
		fType := field.Tag.Get("ddbtype")
		if fName == "" {
			fName = field.Name // no name tag, use field name
		}
		if fType == "" {
			// no type tag, guess using primitive
		}
		switch field.Type.Kind() {
		case reflect.Bool:
			_ = BINARY
		case reflect.String:
			attrs = append(attrs, Attribute{
				Name:  fName,
				Type:  STRING,
				Value: reflect.ValueOf(field).String(),
			})
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Float32, reflect.Float64:
			_ = NUMBER
		case reflect.Slice:
			switch reflect.SliceOf(field.Type).Kind() { // switch on this as well
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Float32, reflect.Float64:
				_ = NUMBER_SET
			case reflect.String:
				_ = STRING_SET
			default:
				// skip field
			}
		default:
			// skip field
		}
	}
	return
}
