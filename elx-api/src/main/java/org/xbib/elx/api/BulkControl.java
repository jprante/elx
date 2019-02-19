package org.xbib.elx.api;

import java.util.Map;
import java.util.Set;

public interface BulkControl {

    void startBulk(String indexName, long startRefreshInterval, long stopRefreshInterval);

    boolean isBulk(String indexName);

    void finishBulk(String indexName);

    Set<String> indices();

    Map<String, Long> getStartBulkRefreshIntervals();

    Map<String, Long> getStopBulkRefreshIntervals();

    String getMaxWaitTime();
}
