package schema

import (
	"encoding/binary"
	"time"
)

func merge(payload []byte, timestamp uint64) (merged []byte) {
	ts := make([]byte, 8)
	binary.LittleEndian.PutUint64(ts, timestamp)
	merged = append(ts, payload[:]...)
	return merged
}

func split(merged []byte) (payload []byte, timestamp uint64) {
	if len(merged) < 9 {
		return payload, 0
	}
	payload = merged[8:]
	ts := merged[0:8]
	timestamp = binary.LittleEndian.Uint64(ts)
	return payload, timestamp
}

func NewSKV(key []byte, value []byte) *StructuredKeyValue {
	return &StructuredKeyValue{
		Key: key,
		Value: &Content{
			Timestamp: uint64(time.Now().Unix()),
			Payload:   value,
		},
	}
}

func (item *Item) ToSItem() *StructuredItem {
	payload, ts := split(item.Value)
	return &StructuredItem{
		Index: item.Index,
		Key:   item.Key,
		Value: &Content{
			Payload:   payload,
			Timestamp: ts,
		},
	}
}

func (item *StructuredItem) ToItem() *Item {
	return &Item{
		Key:   item.Key,
		Value: merge(item.Value.Payload, item.Value.Timestamp),
		Index: item.Index,
	}
}

func (item *SafeItem) ToSafeSItem() *SafeStructuredItem {
	return &SafeStructuredItem{
		Item:  item.Item.ToSItem(),
		Proof: item.Proof,
	}
}

func (list *ItemList) ToSItemList() *StructuredItemList {
	slist := &StructuredItemList{}
	for _, item := range list.Items {
		slist.Items = append(slist.Items, item.ToSItem())
	}
	return slist
}

func (skv *StructuredKeyValue) ToKV() *KeyValue {
	return &KeyValue{
		Key:   skv.Key,
		Value: merge(skv.Value.Payload, skv.Value.Timestamp),
	}
}

func (skvl *SKVList) ToKVList() *KVList {
	kvl := &KVList{}
	for _, v := range skvl.SKVs {
		kvl.KVs = append(kvl.KVs, v.ToKV())
	}
	return kvl
}
