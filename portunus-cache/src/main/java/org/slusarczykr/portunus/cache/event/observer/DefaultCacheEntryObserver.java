package org.slusarczykr.portunus.cache.event.observer;

import org.slusarczykr.portunus.cache.Cache;
import org.slusarczykr.portunus.cache.event.CacheEventListener;
import org.slusarczykr.portunus.cache.event.CacheEventListenerProvider;
import org.slusarczykr.portunus.cache.event.CacheEventType;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static org.slusarczykr.portunus.cache.event.CacheEventType.ACCESS;
import static org.slusarczykr.portunus.cache.event.CacheEventType.ADD;
import static org.slusarczykr.portunus.cache.event.CacheEventType.REMOVE;
import static org.slusarczykr.portunus.cache.event.CacheEventType.UPDATE;

public class DefaultCacheEntryObserver<K, V> implements CacheEntryObserver<K, V>, CacheEventListenerProvider {

    private static final CacheEventListener IDLE_EVENT_LISTENER = () -> {
    };

    private final Map<CacheEventType, CacheEventListener> eventListeners = new ConcurrentHashMap<>();

    @Override
    public void onAccess(Cache.Entry<K, V> entry) {
        get(ACCESS).onEvent();
    }

    @Override
    public void onAdd(Cache.Entry<K, V> entry) {
        get(ADD).onEvent();
    }

    @Override
    public void onUpdate(Cache.Entry<K, V> entry) {
        get(UPDATE).onEvent();
    }

    @Override
    public void onRemove(Cache.Entry<K, V> entry) {
        get(REMOVE).onEvent();
    }

    @Override
    public CacheEventListener get(CacheEventType eventType) {
        return Optional.ofNullable(eventListeners.get(eventType))
                .orElse(IDLE_EVENT_LISTENER);
    }

    @Override
    public void register(CacheEventType eventType, CacheEventListener eventListener) {
        eventListeners.put(eventType, eventListener);
    }

    @Override
    public void unregister(CacheEventType eventType) {
        eventListeners.remove(eventType);
    }
}
