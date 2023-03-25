package org.slusarczykr.portunus.cache.cluster;

import java.util.List;

public interface ServiceLoader {

    void loadServices();

    List<Service> getServices();

    <T extends Service> T getService(Class<T> clazz);

    static void load() {
        DefaultServiceLoader.getInstance().loadServices();
    }
}
