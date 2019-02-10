package org.xbib.elasticsearch.client;

/**
 *
 */
public interface Parameters {

    int DEFAULT_MAX_ACTIONS_PER_REQUEST = 1000;

    int DEFAULT_MAX_CONCURRENT_REQUESTS = Runtime.getRuntime().availableProcessors() * 4;

    String DEFAULT_MAX_VOLUME_PER_REQUEST = "10mb";

    String DEFAULT_FLUSH_INTERVAL = "30s";

    String MAX_ACTIONS_PER_REQUEST = "max_actions_per_request";

    String MAX_CONCURRENT_REQUESTS = "max_concurrent_requests";

    String MAX_VOLUME_PER_REQUEST = "max_volume_per_request";

    String FLUSH_INTERVAL = "flush_interval";
}
