package org.slusarczykr.portunus.cache.util.resource;

import java.io.IOException;

@FunctionalInterface
public interface ResourceLoader {

    <T> T load(String fileName, Class<T> clazz) throws IOException;
}
