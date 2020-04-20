package schema

import (
	"github.com/golang/protobuf/proto"
)

// Merge return a marshalled content object
func Merge(payload []byte, timestamp uint64) (merged []byte, err error) {
	c := &Content{
		Payload:   payload,
		Timestamp: timestamp,
	}
	merged, err = proto.Marshal(c)
	return merged, err
}

// ToSItem return StructuredItem from the receiver
func (item *Item) ToSItem() (*StructuredItem, error) {
	c := Content{}
	err := proto.Unmarshal(item.Value, &c)
	if err != nil {
		return nil, err
	}

	return &StructuredItem{
		Index: item.Index,
		Key:   item.Key,
		Value: &Content{
			Payload:   c.Payload,
			Timestamp: c.Timestamp,
		},
	}, nil
}

// ToItem return Item from the receiver
func (item *StructuredItem) ToItem() (*Item, error) {
	m, err := Merge(item.Value.Payload, item.Value.Timestamp)
	if err != nil {
		return nil, err
	}
	return &Item{
		Key:   item.Key,
		Value: m,
		Index: item.Index,
	}, nil
}

// ToSafeSItem return a SafeStructuredItem from the receiver
func (item *SafeItem) ToSafeSItem() (*SafeStructuredItem, error) {
	i, err := item.Item.ToSItem()
	return &SafeStructuredItem{
			Item:  i,
			Proof: item.Proof,
		},
		err
}

// ToSItemList return a StructuredItemList from the receiver
func (list *ItemList) ToSItemList() (*StructuredItemList, error) {
	slist := &StructuredItemList{}
	for _, item := range list.Items {
		i, err := item.ToSItem()
		if err != nil {
			return nil, err
		}
		slist.Items = append(slist.Items, i)
	}
	return slist, nil
}

// ToKV return a KeyValue from the receiver
func (skv *StructuredKeyValue) ToKV() (*KeyValue, error) {
	m, err := proto.Marshal(skv.Value)
	if err != nil {
		return nil, err
	}
	return &KeyValue{
		Key:   skv.Key,
		Value: m,
	}, nil
}

// ToKVList return a KVList from the receiver
func (skvl *SKVList) ToKVList() (*KVList, error) {
	kvl := &KVList{}
	for _, v := range skvl.SKVs {
		m, err := v.ToKV()
		if err != nil {
			return nil, err
		}
		kvl.KVs = append(kvl.KVs, m)
	}
	return kvl, nil
}

func (list *Page) ToSPage() (*SPage, error) {
	slist := &SPage{}
	for _, item := range list.Items {
		i, err := item.ToSItem()
		if err != nil {
			return nil, err
		}
		slist.Items = append(slist.Items, i)
	}
	return slist, nil
}
