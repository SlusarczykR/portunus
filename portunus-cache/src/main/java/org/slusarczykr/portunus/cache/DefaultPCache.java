package org.slusarczykr.portunus.cache;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.Configuration;
import javax.cache.integration.CompletionListener;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class DefaultPCache<K, V> implements Cache<K, V> {

    public V get(K k) {
        return null;
    }

    public Map<K, V> getAll(Set<? extends K> set) {
        return null;
    }

    public boolean containsKey(K k) {
        return false;
    }

    public void loadAll(Set<? extends K> set, boolean b, CompletionListener completionListener) {

    }

    public void put(K k, V v) {

    }

    public V getAndPut(K k, V v) {
        return null;
    }

    public void putAll(Map<? extends K, ? extends V> map) {

    }

    public boolean putIfAbsent(K k, V v) {
        return false;
    }

    public boolean remove(K k) {
        return false;
    }

    public boolean remove(K k, V v) {
        return false;
    }

    public V getAndRemove(K k) {
        return null;
    }

    public boolean replace(K k, V v, V v1) {
        return false;
    }

    public boolean replace(K k, V v) {
        return false;
    }

    public V getAndReplace(K k, V v) {
        return null;
    }

    public void removeAll(Set<? extends K> set) {

    }

    public void removeAll() {

    }

    public void clear() {

    }

    public <C extends Configuration<K, V>> C getConfiguration(Class<C> aClass) {
        return null;
    }

    public <T> T invoke(K k, EntryProcessor<K, V, T> entryProcessor, Object... objects) throws EntryProcessorException {
        return null;
    }

    public <T> Map<K, EntryProcessorResult<T>> invokeAll(Set<? extends K> set, EntryProcessor<K, V, T> entryProcessor, Object... objects) {
        return null;
    }

    public String getName() {
        return null;
    }

    public CacheManager getCacheManager() {
        return null;
    }

    public void close() {

    }

    public boolean isClosed() {
        return false;
    }

    public <T> T unwrap(Class<T> aClass) {
        return null;
    }

    public void registerCacheEntryListener(CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration) {

    }

    public void deregisterCacheEntryListener(CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration) {

    }

    public Iterator<Entry<K, V>> iterator() {
        return null;
    }
}
