package org.xbib.elx.common;

public enum Parameters {

    DATE_TIME_FORMAT("dateTimeFormat", String.class, "yyyyMMdd"),

    MAX_WAIT_BULK_RESPONSE("bulk.max_wait_response", String.class, "30s"),

    START_BULK_REFRESH_SECONDS("bulk.start_refresh_seconds", Integer.class, -1),

    STOP_BULK_REFRESH_SECONDS("bulk.stop_refresh_seconds", Integer.class, 30),

    ENABLE_BULK_LOGGING("bulk.logging.enabled", Boolean.class, true),

    FAIL_ON_BULK_ERROR("bulk.fail_on_error", Boolean.class, true),

    MAX_ACTIONS_PER_REQUEST("bulk.max_actions_per_request", Integer.class, -1),

    MIN_VOLUME_PER_REQUEST("bulk.min_volume_per_request", String.class, "1k"),

    MAX_VOLUME_PER_REQUEST("bulk.max_volume_per_request", String.class, "5m"),

    FLUSH_INTERVAL("bulk.flush_interval", String.class, "30s"),

    MEASURE_INTERVAL("bulk.measure_interval", String.class, "1s"),

    METRIC_LOG_INTERVAL("bulk.metric_log_interval", String.class, "10s"),

    RING_BUFFER_SIZE("bulk.ring_buffer_size", Integer.class, Runtime.getRuntime().availableProcessors()),

    PERMITS("bulk.permits", Integer.class, Runtime.getRuntime().availableProcessors() - 1);

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
