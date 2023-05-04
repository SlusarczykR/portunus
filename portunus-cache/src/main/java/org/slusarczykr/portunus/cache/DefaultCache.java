package org.slusarczykr.portunus.cache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slusarczykr.portunus.cache.event.CacheEventListener;
import org.slusarczykr.portunus.cache.event.CacheEventType;
import org.slusarczykr.portunus.cache.event.observer.DefaultCacheEntryObserver;
import org.slusarczykr.portunus.cache.exception.OperationFailedException;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class DefaultCache<K, V> implements Cache<K, V> {

    private static final Logger log = LoggerFactory.getLogger(DefaultCache.class);

    private final String name;

    private final Map<K, Cache.Entry<K, V>> cache = new ConcurrentHashMap<>();
    private final DefaultCacheEntryObserver<K, V> cacheEntryObserver = new DefaultCacheEntryObserver<>();

    public DefaultCache(String name) {
        this(name, new HashMap<>());
    }

    public DefaultCache(String name, Map<CacheEventType, CacheEventListener> eventListeners) {
        this.name = name;
        eventListeners.forEach(cacheEntryObserver::register);
    }

    @Override
    public String getName() {
        return name;
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
    public void putAll(Set<Cache.Entry<K, V>> entries) {
        Map<K, V> cacheEntries = entries.stream()
                .collect(Collectors.toMap(Cache.Entry::getKey, Cache.Entry::getValue));
        putAll(cacheEntries);
    }

    @Override
    public void putAll(Map<K, V> entries) {
        validate(entries);

        entries.forEach((key, value) -> {
            Entry<K, V> entry = new Entry<>(key, value);
            cacheEntryObserver.onAdd(entry);
            cache.put(key, entry);
        });
    }

    private void validate(Map<K, V> entries) {
        entries.forEach(this::validate);
    }

    @Override
    public Cache.Entry<K, V> remove(K key) {
        return Optional.ofNullable(cache.remove(key))
                .map(it -> {
                    cacheEntryObserver.onRemove(it);
                    return it;
                })
                .orElseThrow(() -> new OperationFailedException("Entry is not present"));
    }

    @Override
    public void removeAll(Collection<K> keys) {
        keys.forEach(this::remove);
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
