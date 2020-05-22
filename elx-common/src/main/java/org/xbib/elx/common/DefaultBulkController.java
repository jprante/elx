package org.xbib.elx.common;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.xbib.elx.api.BulkController;
import org.xbib.elx.api.BulkListener;
import org.xbib.elx.api.BulkMetric;
import org.xbib.elx.api.BulkProcessor;
import org.xbib.elx.api.ExtendedClient;
import org.xbib.elx.api.IndexDefinition;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class DefaultBulkController implements BulkController {

    private static final Logger logger = LogManager.getLogger(DefaultBulkController.class);

    private final ExtendedClient client;

    private final BulkMetric bulkMetric;

    private BulkProcessor bulkProcessor;

    private final List<String> indexNames;

    private final Map<String, Long> startBulkRefreshIntervals;

    private final Map<String, Long> stopBulkRefreshIntervals;

    private final long maxWaitTime;

    private final TimeUnit maxWaitTimeUnit;

    private final AtomicBoolean active;

    public DefaultBulkController(ExtendedClient client) {
        this.client = client;
        this.bulkMetric = new DefaultBulkMetric();
        this.indexNames = new ArrayList<>();
        this.active = new AtomicBoolean(false);
        this.startBulkRefreshIntervals = new HashMap<>();
        this.stopBulkRefreshIntervals = new HashMap<>();
        this.maxWaitTime = 30L;
        this.maxWaitTimeUnit = TimeUnit.SECONDS;
    }

    @Override
    public BulkMetric getBulkMetric() {
        return bulkMetric;
    }

    @Override
    public Throwable getLastBulkError() {
        return bulkProcessor.getBulkListener().getLastBulkError();
    }

    @Override
    public void init(Settings settings) {
        int maxActionsPerRequest = settings.getAsInt(Parameters.MAX_ACTIONS_PER_REQUEST.name(),
                Parameters.DEFAULT_MAX_ACTIONS_PER_REQUEST.getNum());
        int maxConcurrentRequests = settings.getAsInt(Parameters.MAX_CONCURRENT_REQUESTS.name(),
                Parameters.DEFAULT_MAX_CONCURRENT_REQUESTS.getNum());
        TimeValue flushIngestInterval = settings.getAsTime(Parameters.FLUSH_INTERVAL.name(),
                TimeValue.timeValueSeconds(Parameters.DEFAULT_FLUSH_INTERVAL.getNum()));
        ByteSizeValue maxVolumePerRequest = settings.getAsBytesSize(Parameters.MAX_VOLUME_PER_REQUEST.name(),
                ByteSizeValue.parseBytesSizeValue(Parameters.DEFAULT_MAX_VOLUME_PER_REQUEST.getString(),
                        "maxVolumePerRequest"));
        boolean enableBulkLogging = settings.getAsBoolean(Parameters.ENABLE_BULK_LOGGING.name(),
                Parameters.ENABLE_BULK_LOGGING.getValue());
        BulkListener bulkListener = new DefaultBulkListener(this, bulkMetric, enableBulkLogging);
        this.bulkProcessor = DefaultBulkProcessor.builder(client.getClient(), bulkListener)
                .setBulkActions(maxActionsPerRequest)
                .setConcurrentRequests(maxConcurrentRequests)
                .setFlushInterval(flushIngestInterval)
                .setBulkSize(maxVolumePerRequest)
                .build();
        if (logger.isInfoEnabled()) {
            logger.info("bulk processor up with maxActionsPerRequest = {} maxConcurrentRequests = {} " +
                            "flushIngestInterval = {} maxVolumePerRequest = {}, bulk logging = {}",
                    maxActionsPerRequest, maxConcurrentRequests, flushIngestInterval, maxVolumePerRequest,
                    enableBulkLogging);
        }
        this.active.set(true);
    }

    @Override
    public void inactivate() {
        this.active.set(false);
    }

    @Override
    public void startBulkMode(IndexDefinition indexDefinition) throws IOException {
        startBulkMode(indexDefinition.getFullIndexName(), indexDefinition.getStartRefreshInterval(),
                indexDefinition.getStopRefreshInterval());
    }

    @Override
    public void startBulkMode(String indexName,
                              long startRefreshIntervalInSeconds,
                              long stopRefreshIntervalInSeconds) throws IOException {
        if (!indexNames.contains(indexName)) {
            indexNames.add(indexName);
            startBulkRefreshIntervals.put(indexName, startRefreshIntervalInSeconds);
            stopBulkRefreshIntervals.put(indexName, stopRefreshIntervalInSeconds);
            if (startRefreshIntervalInSeconds != 0L) {
                client.updateIndexSetting(indexName, "refresh_interval", startRefreshIntervalInSeconds + "s",
                        30L, TimeUnit.SECONDS);
            }
        }
    }

    @Override
    public void bulkIndex(IndexRequest indexRequest) {
        ensureActiveAndBulk();
        try {
            bulkMetric.getCurrentIngest().inc(indexRequest.index(), indexRequest.type(), indexRequest.id());
            bulkProcessor.add(indexRequest);
        } catch (Exception e) {
            if (logger.isErrorEnabled()) {
                logger.error("bulk add of index failed: " + e.getMessage(), e);
            }
            inactivate();
        }
    }

    @Override
    public void bulkDelete(DeleteRequest deleteRequest) {
        if (!active.get()) {
            throw new IllegalStateException("inactive");
        }
        try {
            bulkMetric.getCurrentIngest().inc(deleteRequest.index(), deleteRequest.type(), deleteRequest.id());
            bulkProcessor.add(deleteRequest);
        } catch (Exception e) {
            if (logger.isErrorEnabled()) {
                logger.error("bulk add of delete failed: " + e.getMessage(), e);
            }
            inactivate();
        }
    }

    @Override
    public void bulkUpdate(UpdateRequest updateRequest) {
        if (!active.get()) {
            throw new IllegalStateException("inactive");
        }
        try {
            bulkMetric.getCurrentIngest().inc(updateRequest.index(), updateRequest.type(), updateRequest.id());
            bulkProcessor.add(updateRequest);
        } catch (Exception e) {
            if (logger.isErrorEnabled()) {
                logger.error("bulk add of update failed: " + e.getMessage(), e);
            }
            inactivate();
        }
    }

    @Override
    public boolean waitForBulkResponses(long timeout, TimeUnit timeUnit) {
        try {
            return bulkProcessor.awaitFlush(timeout, timeUnit);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("interrupted");
            return false;
        }
    }

    @Override
    public void stopBulkMode(IndexDefinition indexDefinition) throws IOException {
        stopBulkMode(indexDefinition.getFullIndexName(),
                indexDefinition.getMaxWaitTime(), indexDefinition.getMaxWaitTimeUnit());
    }

    @Override
    public void stopBulkMode(String index, long timeout, TimeUnit timeUnit) throws IOException {
        flush();
        if (waitForBulkResponses(timeout, timeUnit)) {
            if (indexNames.contains(index)) {
                Long secs = stopBulkRefreshIntervals.get(index);
                if (secs != null && secs != 0L) {
                    client.updateIndexSetting(index, "refresh_interval", secs + "s",
                            30L, TimeUnit.SECONDS);
                }
                indexNames.remove(index);
            }
        }
    }

    @Override
    public void flush() throws IOException {
        if (bulkProcessor != null) {
            bulkProcessor.flush();
        }
    }

    @Override
    public void close() throws IOException {
        flush();
        bulkMetric.close();
        if (client.waitForResponses(maxWaitTime, maxWaitTimeUnit)) {
            for (String index : indexNames) {
                Long secs = stopBulkRefreshIntervals.get(index);
                if (secs != null && secs != 0L)
                client.updateIndexSetting(index, "refresh_interval", secs + "s",
                        30L, TimeUnit.SECONDS);
            }
            indexNames.clear();
        }
        if (bulkProcessor != null) {
            bulkProcessor.close();
        }
    }

    private void ensureActiveAndBulk() {
        if (!active.get()) {
            throw new IllegalStateException("inactive");
        }
        if (bulkProcessor == null) {
            throw new UnsupportedOperationException("bulk processor not present");
        }
    }
}
