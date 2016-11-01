package org.xbib.elasticsearch.extras.client;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 */
public class SimpleBulkControl implements BulkControl {

    private final Set<String> indexNames = new HashSet<>();

    private final Map<String, Long> startBulkRefreshIntervals = new HashMap<>();

    private final Map<String, Long> stopBulkRefreshIntervals = new HashMap<>();

    @Override
    public void startBulk(String indexName, long startRefreshInterval, long stopRefreshInterval) {
        synchronized (indexNames) {
            indexNames.add(indexName);
            startBulkRefreshIntervals.put(indexName, startRefreshInterval);
            stopBulkRefreshIntervals.put(indexName, stopRefreshInterval);
        }
    }

    @Override
    public boolean isBulk(String indexName) {
        return indexNames.contains(indexName);
    }

    @Override
    public void finishBulk(String indexName) {
        synchronized (indexNames) {
            indexNames.remove(indexName);
        }
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

}
