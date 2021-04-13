package org.xbib.elx.common;

public enum Parameters {

    MAX_WAIT_BULK_RESPONSE_SECONDS("bulk.max_wait_response_seconds", Integer.class, 30),

    START_BULK_REFRESH_SECONDS("bulk.start_refresh_seconds", Integer.class, 0),

    STOP_BULK_REFRESH_SECONDS("bulk.stop_refresh_seconds", Integer.class, 30),

    ENABLE_BULK_LOGGING("bulk.logging.enabled", Boolean.class, true),

    FAIL_ON_BULK_ERROR("bulk.failonerror", Boolean.class, true),

    MAX_ACTIONS_PER_REQUEST("bulk.max_actions_per_request", Integer.class, 1000),

    // 0 = 1 CPU, synchronous requests, &gt; 0 = n + 1 CPUs, asynchronous requests
    MAX_CONCURRENT_REQUESTS("bulk.max_concurrent_requests", Integer.class, Runtime.getRuntime().availableProcessors() - 1),

    MAX_VOLUME_PER_REQUEST("bulk.max_volume_per_request", String.class, "1mb"),

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
