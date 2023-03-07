package org.slusarczykr.portunus.cache.jcache.provider;

import javax.cache.CacheManager;
import javax.cache.configuration.OptionalFeature;
import java.net.URI;
import java.util.Properties;

public class JCacheProvider implements javax.cache.spi.CachingProvider {

    public CacheManager getCacheManager(URI uri, ClassLoader classLoader, Properties properties) {
        return null;
    }

    public ClassLoader getDefaultClassLoader() {
        return null;
    }

    public URI getDefaultURI() {
        return null;
    }

    public Properties getDefaultProperties() {
        return null;
    }

    public CacheManager getCacheManager(URI uri, ClassLoader classLoader) {
        return null;
    }

    public CacheManager getCacheManager() {
        return null;
    }

    public void close() {

    }

    public void close(ClassLoader classLoader) {

    }

    public void close(URI uri, ClassLoader classLoader) {

    }

    public boolean isSupported(OptionalFeature optionalFeature) {
        return false;
    }
}
