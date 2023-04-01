package org.slusarczykr.portunus.cache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slusarczykr.portunus.cache.event.CacheEventListener;
import org.slusarczykr.portunus.cache.event.CacheEventType;
import org.slusarczykr.portunus.cache.event.observer.DefaultCacheEntryObserver;
import org.slusarczykr.portunus.cache.exception.PortunusException;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class DefaultCache<K, V> implements Cache<K, V> {

    private static final Logger log = LoggerFactory.getLogger(DefaultCache.class);

    private final Map<K, Cache.Entry<K, V>> cache = new ConcurrentHashMap<>();
    private final DefaultCacheEntryObserver<K, V> cacheEntryObserver = new DefaultCacheEntryObserver<>();

    public DefaultCache(Map<CacheEventType, CacheEventListener> eventListeners) {
        eventListeners.forEach(cacheEntryObserver::register);
    }

    @Override
    public boolean isEmpty() {
        return cache.isEmpty();
    }

    @Override
    public boolean containsKey(K key) {
        return cache.containsKey(key);
    }

    @Override
    public boolean containsValue(V value) {
        return cache.entrySet().stream()
                .anyMatch(it -> it.getValue().equals(value));
    }

    public Optional<Cache.Entry<K, V>> getEntry(K key) {
        return Optional.ofNullable(cache.get(key))
                .map(it -> {
                    cacheEntryObserver.onAccess(it);
                    return it;
                });
    }

    public Collection<Cache.Entry<K, V>> getEntries(Collection<K> keys) {
        return keys.stream()
                .map(this::getEntry)
                .map(Optional::stream)
                .map(it -> (Cache.Entry<K, V>) it)
                .toList();

    }

    public Collection<Cache.Entry<K, V>> allEntries() {
        Collection<Cache.Entry<K, V>> entries = cache.values();
        entries.forEach(cacheEntryObserver::onAccess);

        return Collections.unmodifiableCollection(entries);
    }

    @Override
    public void put(K key, V value) {
        validate(key, value);
        Entry<K, V> entry = new Entry<>(key, value);

        if (!cache.containsKey(key)) {
            cache.put(key, entry);
            cacheEntryObserver.onAdd(entry);
        }
    }

    @Override
    public void putAll(Map<K, V> entries) {
        entries.forEach(this::validate);
        entries.forEach((key, value) -> {
            Entry<K, V> entry = new Entry<>(key, value);
            cacheEntryObserver.onAdd(entry);
            cache.put(key, entry);
        });
    }

    @Override
    public Cache.Entry<K, V> remove(K key) throws PortunusException {
        return Optional.ofNullable(cache.remove(key))
                .map(it -> {
                    cacheEntryObserver.onRemove(it);
                    return it;
                })
                .orElseThrow(() -> new PortunusException("Entry is not present"));
    }

    @Override
    public void removeAll(Collection<K> keys) {
        keys.forEach(it -> {
            try {
                remove(it);
            } catch (PortunusException e) {
                log.error("Could not remove entry", e);
            }
        });
    }

    private void validate(K key, V value) {
        Objects.requireNonNull(key);
        Objects.requireNonNull(value);
    }

    public record Entry<K, V>(K key, V value) implements Cache.Entry<K, V> {

        public Entry(Map.Entry<K, V> entry) {
            this(entry.getKey(), entry.getValue());
        }

        @Override
        public K getKey() {
            return key;
        }

        @Override
        public V getValue() {
            return value;
        }
    }
}
