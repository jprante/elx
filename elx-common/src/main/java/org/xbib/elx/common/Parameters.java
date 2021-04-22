package org.xbib.elx.common;

public enum Parameters {

    CLUSTER_TARGET_HEALTH("cluster.target_health", String.class, "GREEN"),

    CLUSTER_TARGET_HEALTH_TIMEOUT("cluster.target_health_timeout", String.class, "30m"),

    DATE_TIME_FORMAT("dateTimeFormat", String.class, "yyyyMMdd"),

    BULK_START_REFRESH_SECONDS("bulk.start_refresh_seconds", Integer.class, -1),

    BULK_STOP_REFRESH_SECONDS("bulk.stop_refresh_seconds", Integer.class, 30),

    BULK_LOGGING_ENABLED("bulk.logging.enabled", Boolean.class, true),

    BULK_FAIL_ON_ERROR("bulk.fail_on_error", Boolean.class, true),

    BULK_MAX_ACTIONS_PER_REQUEST("bulk.max_actions_per_request", Integer.class, -1),

    BULK_MIN_VOLUME_PER_REQUEST("bulk.min_volume_per_request", String.class, "1k"),

    BULK_MAX_VOLUME_PER_REQUEST("bulk.max_volume_per_request", String.class, "1m"),

    BULK_FLUSH_INTERVAL("bulk.flush_interval", String.class, "30s"),

    BULK_MEASURE_INTERVAL("bulk.measure_interval", String.class, "1s"),

    BULK_METRIC_LOG_INTERVAL("bulk.metric_log_interval", String.class, "10s"),

    BULK_RING_BUFFER_SIZE("bulk.ring_buffer_size", Integer.class, Runtime.getRuntime().availableProcessors()),

    BULK_PERMITS("bulk.permits", Integer.class, Runtime.getRuntime().availableProcessors() - 1),

    SEARCH_METRIC_LOG_INTERVAL("search.metric_log_interval", String.class, "10s");

    private final String name;

    private final Class<?> type;

    private final Object value;

    Parameters(String name, Class<?> type, Object value) {
        this.name = name;
        this.type = type;
        this.value = value;
    }

    public String getName() {
        return name;
    }

    public Class<?> getType() {
        return type;
    }

    public Boolean getBoolean() {
        return type == Boolean.class ? (Boolean) value : Boolean.FALSE;
    }

    public Integer getInteger() {
        return type == Integer.class ? (Integer) value : 0;
    }

    public String getString() {
        return type == String.class ? (String)  value : null;
    }
}
