package org.slusarczykr.portunus.cache.event.observer;

import org.slusarczykr.portunus.cache.Cache;

public interface CacheEntryObserver<K, V> {

    void onAccess(Cache.Entry<K, V> entry);

    void onAdd(Cache.Entry<K, V> entry);

    void onUpdate(Cache.Entry<K, V> entry);

    void onRemove(Cache.Entry<K, V> entry);
}
