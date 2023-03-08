package org.slusarczykr.portunus.cache.event;

public interface CacheEventListenerKeeper {

    CacheEventListener get(CacheEventType eventType);

    void register(CacheEventType eventType, CacheEventListener eventListener);

    void unregister(CacheEventType eventType);
}
