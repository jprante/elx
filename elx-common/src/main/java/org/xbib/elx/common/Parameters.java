package org.xbib.elx.common;

public enum Parameters {

    DEFAULT_MAX_ACTIONS_PER_REQUEST(1000),

    DEFAULT_MAX_CONCURRENT_REQUESTS(Runtime.getRuntime().availableProcessors()),

    DEFAULT_MAX_VOLUME_PER_REQUEST("10mb"),

    DEFAULT_FLUSH_INTERVAL("30s"),

    MAX_ACTIONS_PER_REQUEST ("max_actions_per_request"),

    MAX_CONCURRENT_REQUESTS("max_concurrent_requests"),

    MAX_VOLUME_PER_REQUEST("max_volume_per_request"),

    FLUSH_INTERVAL("flush_interval");

    int num;

    String string;

    Parameters(int num) {
        this.num = num;
    }

    Parameters(String string) {
        this.string = string;
    }

    int getNum() {
        return  num;
    }

    String getString() {
        return string;
    }
}
