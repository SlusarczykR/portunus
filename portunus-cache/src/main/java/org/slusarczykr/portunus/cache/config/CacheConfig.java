package org.slusarczykr.portunus.cache.config;

public interface CacheConfig<K, V> {

    Class<K> getKeyType();

    Class<V> getValueType();
}
