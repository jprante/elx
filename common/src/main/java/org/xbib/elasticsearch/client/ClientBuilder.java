package org.xbib.elasticsearch.client;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

/**
 *
 */
public final class ClientBuilder implements Parameters {

    private final Settings.Builder settingsBuilder;

    private Map<Class<? extends ClientMethods>, ClientMethods> clientMethodsMap;

    private BulkMetric metric;

    private BulkControl control;

    public ClientBuilder() {
        this(Thread.currentThread().getContextClassLoader());
    }

    public ClientBuilder(ClassLoader classLoader) {
        this.settingsBuilder = Settings.builder();
        //settingsBuilder.put("node.name", "clientnode");
        this.clientMethodsMap = new HashMap<>();
        ServiceLoader<ClientMethods> serviceLoader = ServiceLoader.load(ClientMethods.class,
                classLoader != null ? classLoader : Thread.currentThread().getContextClassLoader());
        for (ClientMethods clientMethods : serviceLoader) {
            clientMethodsMap.put(clientMethods.getClass(), clientMethods);
        }
    }

    public static ClientBuilder builder() {
        return new ClientBuilder();
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

    public ClientBuilder setMetric(BulkMetric metric) {
        this.metric = metric;
        return this;
    }

    public ClientBuilder setControl(BulkControl control) {
        this.control = control;
        return this;
    }

    public <C extends ClientMethods> C getClient(Class<C> clientClass) throws IOException {
        return getClient(null, clientClass);
    }

    @SuppressWarnings("unchecked")
    public <C extends ClientMethods> C getClient(Client client, Class<C> clientClass) throws IOException {
        Settings settings = settingsBuilder.build();
        return (C) clientMethodsMap.get(clientClass)
                .maxActionsPerRequest(settings.getAsInt(MAX_ACTIONS_PER_REQUEST, DEFAULT_MAX_ACTIONS_PER_REQUEST))
                .maxConcurrentRequests(settings.getAsInt(MAX_CONCURRENT_REQUESTS, DEFAULT_MAX_CONCURRENT_REQUESTS))
                .maxVolumePerRequest(settings.getAsBytesSize(MAX_VOLUME_PER_REQUEST, DEFAULT_MAX_VOLUME_PER_REQUEST))
                .flushIngestInterval(settings.getAsTime(FLUSH_INTERVAL, DEFAULT_FLUSH_INTERVAL))
                .init(client, settings, metric, control);
    }
}
