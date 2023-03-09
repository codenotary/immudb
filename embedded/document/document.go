package document

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/tidwall/gjson"
)

// Document is a json document
type Document struct {
	result gjson.Result
}

// Documents is an array of documents
type Documents []*Document

// UnmarshalJSON satisfies the json Unmarshaler interface
func (d *Document) UnmarshalJSON(bytes []byte) error {
	doc, err := NewDocumentFromBytes(bytes)
	if err != nil {
		return err
	}
	d.result = doc.result
	return nil
}

// MarshalJSON satisfies the json Marshaler interface
func (d *Document) MarshalJSON() ([]byte, error) {
	return d.Bytes(), nil
}

// NewDocument creates a new json document
func NewDocument() *Document {
	parsed := gjson.Parse("{}")
	return &Document{
		result: parsed,
	}
}

// NewDocumentFromBytes creates a new document from the given json bytes
func NewDocumentFromBytes(json []byte) (*Document, error) {
	if !gjson.ValidBytes(json) {
		return nil, errors.New(fmt.Sprintf("invalid json: %s", string(json)))
	}
	d := &Document{
		result: gjson.ParseBytes(json),
	}
	if !d.Valid() {
		return nil, errors.New("invalid document")
	}
	return d, nil
}

// NewDocumentFrom creates a new document from the given interface
func NewDocumentFrom(value interface{}) (*Document, error) {
	var err error
	bits, err := json.Marshal(value)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("failed to json encode value: %#v", value))
	}
	return NewDocumentFromBytes(bits)
}

// Valid returns whether the document is valid
func (d *Document) Valid() bool {
	return gjson.ValidBytes(d.Bytes()) && !d.result.IsArray()
}

// String returns the document as a json string
func (d *Document) String() string {
	return d.result.String()
}

// Bytes returns the document as json bytes
func (d *Document) Bytes() []byte {
	return []byte(d.result.Raw)
}

func (d *Document) Get(field string) interface{} {
	if d.result.Get(field).Exists() {
		return d.result.Get(field).Value()
	}
	return nil
}

// FieldPaths returns the paths to fields & nested fields in dot notation format
func (d *Document) FieldPaths() []string {
	paths := &[]string{}
	d.paths(d.result, paths)
	return *paths
}

func (d *Document) paths(result gjson.Result, pathValues *[]string) {
	result.ForEach(func(key, value gjson.Result) bool {
		if value.IsObject() {
			d.paths(value, pathValues)
		} else {
			*pathValues = append(*pathValues, value.Path(d.result.Raw))
		}
		return true
	})
}

// Encode encodes the json document to the io writer
func (d *Document) Encode(w io.Writer) error {
	_, err := w.Write(d.Bytes())
	if err != nil {
		return fmt.Errorf("failed to write document: %w", err)
	}
	return nil
}
