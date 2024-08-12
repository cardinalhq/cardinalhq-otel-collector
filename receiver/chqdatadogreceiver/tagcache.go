package datadogreceiver

import (
	"github.com/cardinalhq/cardinalhq-otel-collector/extension/chqtagcacheextension"
)

type localTagCache struct {
	cache map[string][]chqtagcacheextension.Tag
}

func newLocalTagCache() *localTagCache {
	return &localTagCache{
		cache: make(map[string][]chqtagcacheextension.Tag),
	}
}

func (ltc *localTagCache) FetchCache(ext *chqtagcacheextension.CHQTagcacheExtension, key string) []chqtagcacheextension.Tag {
	if tags, ok := ltc.cache[key]; ok {
		return tags
	}
	tags, err := ext.FetchCache(key)
	if err != nil {
		tags = []chqtagcacheextension.Tag{}
	}
	ltc.cache[key] = tags
	return tags
}

func (ltc *localTagCache) PutCache(ext *chqtagcacheextension.CHQTagcacheExtension, key string, tags []chqtagcacheextension.Tag) error {
	ltc.cache[key] = tags
	return ext.PutCache(key, tags)
}
