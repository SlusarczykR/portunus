package org.slusarczykr.portunus.cache.config;

public record DefaultConfig<K, V>(
        Class<K> keyType,
        Class<V> valueType
) implements CacheConfig<K, V> {

    @Override
    public Class<K> getKeyType() {
        return keyType;
    }

    @Override
    public Class<V> getValueType() {
        return valueType;
    }
}
