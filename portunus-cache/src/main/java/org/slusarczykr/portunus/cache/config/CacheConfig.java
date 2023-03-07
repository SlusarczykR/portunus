package org.slusarczykr.portunus.cache.config;

public interface CacheConfig<K, V> {

    K getKeyType();

    V getValueType();
}
