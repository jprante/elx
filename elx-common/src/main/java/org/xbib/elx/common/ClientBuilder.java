package org.xbib.elx.common;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.xbib.elx.api.AdminClientProvider;
import org.xbib.elx.api.BulkClientProvider;
import org.xbib.elx.api.BasicClient;
import org.xbib.elx.api.SearchClientProvider;

import java.io.IOException;
import java.util.Map;
import java.util.ServiceLoader;

@SuppressWarnings("rawtypes")
public class ClientBuilder {

    private static final Logger logger = LogManager.getLogger(ClientBuilder.class);

    private final ElasticsearchClient client;

    private final ClassLoader classLoader;

    private final Settings.Builder settingsBuilder;

    private Class<? extends AdminClientProvider> adminClientProvider;

    private Class<? extends BulkClientProvider> bulkClientProvider;

    private Class<? extends SearchClientProvider> searchClientProvider;

    public ClientBuilder() {
        this(null);
    }

    public ClientBuilder(ElasticsearchClient client) {
        this(client, ClassLoader.getSystemClassLoader());
    }

    public ClientBuilder(ElasticsearchClient client, ClassLoader classLoader) {
        this.client = client;
        this.classLoader = classLoader;
        this.settingsBuilder = Settings.builder();
        settingsBuilder.put("node.name", "elx-client-" + Version.CURRENT);
        for (Parameters p : Parameters.values()) {
            if (p.getType() == Boolean.class) {
                settingsBuilder.put(p.getName(), p.getBoolean());
            }
            if (p.getType() == Integer.class) {
                settingsBuilder.put(p.getName(), p.getInteger());
            }
            if (p.getType() == String.class) {
                settingsBuilder.put(p.getName(), p.getString());
            }
        }
    }

    public static ClientBuilder builder() {
        return new ClientBuilder();
    }

    public static ClientBuilder builder(ElasticsearchClient client) {
        return new ClientBuilder(client);
    }

    public ClientBuilder setAdminClientProvider(Class<? extends AdminClientProvider> adminClientProvider) {
        this.adminClientProvider = adminClientProvider;
        return this;
    }

    public ClientBuilder setBulkClientProvider(Class<? extends BulkClientProvider> bulkClientProvider) {
        this.bulkClientProvider = bulkClientProvider;
        return this;
    }

    public ClientBuilder setSearchClientProvider(Class<? extends SearchClientProvider> searchClientProvider) {
        this.searchClientProvider = searchClientProvider;
        return this;
    }

    public ClientBuilder put(String key, String value) {
        settingsBuilder.put(key, value);
        return this;
    }

    public ClientBuilder put(String key, Integer value) {
        settingsBuilder.put(key, value);
        return this;
    }

    public ClientBuilder put(String key, Long value) {
        settingsBuilder.put(key, value);
        return this;
    }

    public ClientBuilder put(String key, Double value) {
        settingsBuilder.put(key, value);
        return this;
    }

    public ClientBuilder put(String key, ByteSizeValue value) {
        settingsBuilder.put(key, value);
        return this;
    }

    public ClientBuilder put(String key, TimeValue value) {
        settingsBuilder.put(key, value);
        return this;
    }

    public ClientBuilder put(Settings settings) {
        settingsBuilder.put(settings);
        return this;
    }

    public ClientBuilder put(Map<String, ?> map) {
        for (Map.Entry<String, ?> entry : map.entrySet()) {
            if (entry.getValue() == null) {
                continue;
            }
            if (entry.getValue() instanceof String ||
                    entry.getValue() instanceof  Integer ||
                    entry.getValue() instanceof Long ||
                    entry.getValue() instanceof Float ||
                    entry.getValue() instanceof TimeValue) {
                settingsBuilder.put(entry.getKey(), entry.getValue().toString());
            } else {
                logger.log(Level.WARN, "skipping " + entry.getValue() +
                        " because invalid class type " + entry.getValue().getClass().getName());
            }
        }
        return this;
    }

    @SuppressWarnings("unchecked")
    public <C extends BasicClient> C build() throws IOException {
        Settings settings = settingsBuilder.build();
        logger.log(Level.INFO, "settings = " + settings.toDelimitedString(','));
        if (adminClientProvider != null) {
            for (AdminClientProvider provider : ServiceLoader.load(AdminClientProvider.class, classLoader)) {
                if (provider.getClass().isAssignableFrom(adminClientProvider)) {
                    C c = (C) provider.getClient();
                    c.setClient(client);
                    c.init(settings);
                    return c;
                }
            }
        }
        if (bulkClientProvider != null) {
            for (BulkClientProvider provider : ServiceLoader.load(BulkClientProvider.class, classLoader)) {
                if (provider.getClass().isAssignableFrom(bulkClientProvider)) {
                    C c = (C) provider.getClient();
                    c.setClient(client);
                    c.init(settings);
                    return c;
                }
            }
        }
        if (searchClientProvider != null) {
            for (SearchClientProvider provider : ServiceLoader.load(SearchClientProvider.class, classLoader)) {
                if (provider.getClass().isAssignableFrom(searchClientProvider)) {
                    C c = (C) provider.getClient();
                    c.setClient(client);
                    c.init(settings);
                    return c;
                }
            }
        }
        throw new IllegalArgumentException("no provider");
    }
}
