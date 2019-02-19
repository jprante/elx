package org.xbib.elx.common;

import org.xbib.elx.api.BulkControl;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 */
public class SimpleBulkControl implements BulkControl {

    private final Set<String> indexNames;

    private final Map<String, Long> startBulkRefreshIntervals;

    private final Map<String, Long> stopBulkRefreshIntervals;

    private String maxWaitTime;

    public SimpleBulkControl() {
        indexNames = new HashSet<>();
        startBulkRefreshIntervals = new HashMap<>();
        stopBulkRefreshIntervals = new HashMap<>();
        maxWaitTime = "30s";
    }

    @Override
    public void startBulk(String indexName, long startRefreshInterval, long stopRefreshInterval) {
        indexNames.add(indexName);
        startBulkRefreshIntervals.put(indexName, startRefreshInterval);
        stopBulkRefreshIntervals.put(indexName, stopRefreshInterval);
    }

    @Override
    public boolean isBulk(String indexName) {
        return indexNames.contains(indexName);
    }

    @Override
    public void finishBulk(String indexName) {
        indexNames.remove(indexName);
    }

    @Override
    public Set<String> indices() {
        return indexNames;
    }

    @Override
    public Map<String, Long> getStartBulkRefreshIntervals() {
        return startBulkRefreshIntervals;
    }

    @Override
    public Map<String, Long> getStopBulkRefreshIntervals() {
        return stopBulkRefreshIntervals;
    }

    @Override
    public String getMaxWaitTime() {
        return maxWaitTime;
    }

}
