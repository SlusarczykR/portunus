package org.slusarczykr.portunus.cache.config;

import org.slusarczykr.portunus.cache.event.CacheEventListener;
import org.slusarczykr.portunus.cache.event.CacheEventType;

import java.util.Map;

public record DefaultConfig<K, V>(
        Class<K> keyType,
        Class<V> valueType,
        Map<CacheEventType, CacheEventListener> eventListeners
) implements CacheConfig<K, V> {

    @Override
    public Class<K> getKeyType() {
        return keyType;
    }

    @Override
    public Class<V> getValueType() {
        return valueType;
    }

    @Override
    public Map<CacheEventType, CacheEventListener> getEventListeners() {
        return eventListeners;
    }
}
