package org.xbib.elx.common;

public enum Parameters {

    ENABLE_BULK_LOGGING(false),

    DEFAULT_MAX_ACTIONS_PER_REQUEST(1000),

    DEFAULT_MAX_CONCURRENT_REQUESTS(Runtime.getRuntime().availableProcessors()),

    DEFAULT_MAX_VOLUME_PER_REQUEST("10mb"),

    DEFAULT_FLUSH_INTERVAL(30),

    MAX_ACTIONS_PER_REQUEST ("max_actions_per_request"),

    MAX_CONCURRENT_REQUESTS("max_concurrent_requests"),

    MAX_VOLUME_PER_REQUEST("max_volume_per_request"),

    FLUSH_INTERVAL("flush_interval");

    boolean flag;

    int num;

    String string;

    Parameters(boolean flag) {
        this.flag = flag;
    }

    Parameters(int num) {
        this.num = num;
    }

    Parameters(String string) {
        this.string = string;
    }

    boolean getValue() {
        return flag;
    }

    int getNum() {
        return  num;
    }

    String getString() {
        return string;
    }
}
