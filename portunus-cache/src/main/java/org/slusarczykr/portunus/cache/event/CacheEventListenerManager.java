package org.slusarczykr.portunus.cache.event;

public interface CacheEventListenerManager {

    CacheEventListener get(CacheEventType eventType);

    void register(CacheEventType eventType, CacheEventListener eventListener);

    void unregister(CacheEventType eventType);
}
