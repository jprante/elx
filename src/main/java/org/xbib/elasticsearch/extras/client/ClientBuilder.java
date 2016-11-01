package org.xbib.elasticsearch.extras.client;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.xbib.elasticsearch.extras.client.node.BulkNodeClient;
import org.xbib.elasticsearch.extras.client.transport.BulkTransportClient;
import org.xbib.elasticsearch.extras.client.transport.MockTransportClient;

/**
 *
 */
public final class ClientBuilder implements Parameters {

    private final Settings.Builder settingsBuilder;

    private BulkMetric metric;

    private BulkControl control;

    public ClientBuilder() {
        settingsBuilder = Settings.builder();
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

    public BulkNodeClient toBulkNodeClient(Client client) {
        Settings settings = settingsBuilder.build();
        return new BulkNodeClient()
                .maxActionsPerRequest(settings.getAsInt(MAX_ACTIONS_PER_REQUEST, DEFAULT_MAX_ACTIONS_PER_REQUEST))
                .maxConcurrentRequests(settings.getAsInt(MAX_CONCURRENT_REQUESTS, DEFAULT_MAX_CONCURRENT_REQUESTS))
                .maxVolumePerRequest(settings.getAsBytesSize(MAX_VOLUME_PER_REQUEST, DEFAULT_MAX_VOLUME_PER_REQUEST))
                .flushIngestInterval(settings.getAsTime(FLUSH_INTERVAL, DEFAULT_FLUSH_INTERVAL))
                .init(client, metric, control);
    }

    public BulkTransportClient toBulkTransportClient() {
        Settings settings = settingsBuilder.build();
        return new BulkTransportClient()
                .maxActionsPerRequest(settings.getAsInt(MAX_ACTIONS_PER_REQUEST, DEFAULT_MAX_ACTIONS_PER_REQUEST))
                .maxConcurrentRequests(settings.getAsInt(MAX_CONCURRENT_REQUESTS, DEFAULT_MAX_CONCURRENT_REQUESTS))
                .maxVolumePerRequest(settings.getAsBytesSize(MAX_VOLUME_PER_REQUEST, DEFAULT_MAX_VOLUME_PER_REQUEST))
                .flushIngestInterval(settings.getAsTime(FLUSH_INTERVAL, DEFAULT_FLUSH_INTERVAL))
                .init(settings, metric, control);
    }

    public MockTransportClient toMockTransportClient() {
        Settings settings = settingsBuilder.build();
        return new MockTransportClient()
                .maxActionsPerRequest(settings.getAsInt(MAX_ACTIONS_PER_REQUEST, DEFAULT_MAX_ACTIONS_PER_REQUEST))
                .maxConcurrentRequests(settings.getAsInt(MAX_CONCURRENT_REQUESTS, DEFAULT_MAX_CONCURRENT_REQUESTS))
                .maxVolumePerRequest(settings.getAsBytesSize(MAX_VOLUME_PER_REQUEST, DEFAULT_MAX_VOLUME_PER_REQUEST))
                .flushIngestInterval(settings.getAsTime(FLUSH_INTERVAL, DEFAULT_FLUSH_INTERVAL))
                .init(settings, metric, control);
    }

}
