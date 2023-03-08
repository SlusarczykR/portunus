package org.slusarczykr.portunus.cache.config;

import org.slusarczykr.portunus.cache.event.CacheEventListener;
import org.slusarczykr.portunus.cache.event.CacheEventType;

import java.util.Map;

public interface CacheConfig<K, V> {

    Class<K> getKeyType();

    Class<V> getValueType();

    Map<CacheEventType, CacheEventListener> getEventListeners();
}
