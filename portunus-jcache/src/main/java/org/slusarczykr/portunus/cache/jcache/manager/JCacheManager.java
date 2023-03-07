package org.slusarczykr.portunus.cache.jcache.manager;

import javax.cache.Cache;
import javax.cache.configuration.Configuration;
import javax.cache.spi.CachingProvider;
import java.net.URI;
import java.util.Properties;

public class JCacheManager implements javax.cache.CacheManager {

    public CachingProvider getCachingProvider() {
        return null;
    }

    public URI getURI() {
        return null;
    }

    public ClassLoader getClassLoader() {
        return null;
    }

    public Properties getProperties() {
        return null;
    }

    public <K, V, C extends Configuration<K, V>> Cache<K, V> createCache(String s, C c) throws IllegalArgumentException {
        return null;
    }

    public <K, V> Cache<K, V> getCache(String s, Class<K> aClass, Class<V> aClass1) {
        return null;
    }

    public <K, V> Cache<K, V> getCache(String s) {
        return null;
    }

    public Iterable<String> getCacheNames() {
        return null;
    }

    public void destroyCache(String s) {

    }

    public void enableManagement(String s, boolean b) {

    }

    public void enableStatistics(String s, boolean b) {

    }

    public void close() {

    }

    public boolean isClosed() {
        return false;
    }

    public <T> T unwrap(Class<T> aClass) {
        return null;
    }
}
