package kvserver

import "hash/fnv"



type KVStore struct {
	nodeID     string
    buckets    []*Bucket
    numBuckets int
}

func NewKVStore(nodeID string,numBuckets int) *KVStore {
    buckets := make([]*Bucket, numBuckets)
    for i := 0; i < numBuckets; i++ {
        buckets[i] = NewBucket(nodeID, numBuckets)
    }
    return &KVStore{
		nodeID: nodeID,
        buckets:    buckets,
        numBuckets: numBuckets,
    }
}

func (kvs *KVStore) hash(key string) uint32 {
    h := fnv.New32a()
    h.Write([]byte(key))
    return h.Sum32()
}

func (kvs *KVStore) getBucket(key string) *Bucket {
    hash := kvs.hash(key)
    index := int(hash % uint32(kvs.numBuckets))
    return kvs.buckets[index]
}

func (kvs *KVStore) Set(key string, value interface{}) {
    bucket := kvs.getBucket(key)
    bucket.Set(key, value)
}

func (kvs *KVStore) Get(key string) (interface{}, bool) {
    bucket := kvs.getBucket(key)
    return bucket.Get(key)
}

func (kvs *KVStore) Delete(key string) {
    bucket := kvs.getBucket(key)
    bucket.Delete(key)
}

func (kvs *KVStore) Len()int{
    return kvs.numBuckets
}