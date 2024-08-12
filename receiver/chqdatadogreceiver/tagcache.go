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

func (ltc *localTagCache) FetchCache(ext *chqtagcacheextension.CHQTagcacheExtension, apikey string, hostname string) []chqtagcacheextension.Tag {
	if ext == nil {
		return []chqtagcacheextension.Tag{}
	}
	key := apikey + "/" + hostname
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

func (ltc *localTagCache) PutCache(ext *chqtagcacheextension.CHQTagcacheExtension, apikey string, hostname string, tags []chqtagcacheextension.Tag) error {
	if ext == nil {
		return nil
	}
	key := apikey + "/" + hostname
	ltc.cache[key] = tags
	return ext.PutCache(key, tags)
}
