package org.xbib.elx.common;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.xbib.elx.api.ExtendedClient;
import org.xbib.elx.api.ExtendedClientProvider;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

@SuppressWarnings("rawtypes")
public class ClientBuilder {

    private static final Logger logger = LogManager.getLogger(ClientBuilder.class);

    private final ElasticsearchClient client;

    private final Settings.Builder settingsBuilder;

    private Map<Class<? extends ExtendedClientProvider>, ExtendedClientProvider> providerMap;

    private Class<? extends ExtendedClientProvider> provider;

    public ClientBuilder() {
        this(null);
    }

    public ClientBuilder(ElasticsearchClient client) {
        this(client, Thread.currentThread().getContextClassLoader());
    }

    public ClientBuilder(ElasticsearchClient client, ClassLoader classLoader) {
        this.client = client;
        this.settingsBuilder = Settings.builder();
        settingsBuilder.put("node.name", "elx-client-" + Version.CURRENT);
        this.providerMap = new HashMap<>();
        ServiceLoader<ExtendedClientProvider> serviceLoader = ServiceLoader.load(ExtendedClientProvider.class,
                classLoader != null ? classLoader : Thread.currentThread().getContextClassLoader());
        for (ExtendedClientProvider provider : serviceLoader) {
            providerMap.put(provider.getClass(), provider);
        }
    }

    public static ClientBuilder builder() {
        return new ClientBuilder();
    }

    public static ClientBuilder builder(ElasticsearchClient client) {
        return new ClientBuilder(client);
    }

    public ClientBuilder provider(Class<? extends ExtendedClientProvider> provider) {
        this.provider = provider;
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

    @SuppressWarnings("unchecked")
    public <C extends ExtendedClient> C build() throws IOException {
        if (provider == null) {
            throw new IllegalArgumentException("no provider");
        }
        Settings settings = settingsBuilder.build();
        logger.log(Level.INFO, "settings = " + settings.toDelimitedString(','));
        return (C) providerMap.get(provider).getExtendedClient()
                .setClient(client)
                .init(settings);
    }
}
