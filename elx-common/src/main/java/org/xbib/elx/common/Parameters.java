package org.xbib.elx.common;

public enum Parameters {

    DATE_TIME_FORMAT("dateTimeFormat", String.class, "yyyyMMdd"),

    MAX_WAIT_BULK_RESPONSE("bulk.max_wait_response", String.class, "30s"),

    START_BULK_REFRESH_SECONDS("bulk.start_refresh_seconds", Integer.class, -1),

    STOP_BULK_REFRESH_SECONDS("bulk.stop_refresh_seconds", Integer.class, 30),

    ENABLE_BULK_LOGGING("bulk.logging.enabled", Boolean.class, true),

    FAIL_ON_BULK_ERROR("bulk.failonerror", Boolean.class, true),

    MAX_ACTIONS_PER_REQUEST("bulk.max_actions_per_request", Integer.class, -1),

    RESPONSE_TIME_COUNT("bulk.response_time_count", Integer.class, 16),

    MAX_CONCURRENT_REQUESTS("bulk.max_concurrent_requests", Integer.class, 1 /*Runtime.getRuntime().availableProcessors() - 1*/),

    MAX_VOLUME_PER_REQUEST("bulk.max_volume_per_request", String.class, "1kb"),

    FLUSH_INTERVAL("bulk.flush_interval", String.class, "30s");

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
