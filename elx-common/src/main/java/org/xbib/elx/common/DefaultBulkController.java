package org.xbib.elx.common;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.xbib.elx.api.BulkClient;
import org.xbib.elx.api.BulkController;
import org.xbib.elx.api.BulkMetric;
import org.xbib.elx.api.BulkProcessor;
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

    private final BulkClient client;

    private final BulkMetric bulkMetric;

    private final List<String> indexNames;

    private final Map<String, Long> startBulkRefreshIntervals;

    private final Map<String, Long> stopBulkRefreshIntervals;

    private final long maxWaitTime;

    private final TimeUnit maxWaitTimeUnit;

    private BulkProcessor bulkProcessor;

    private BulkListener bulkListener;

    private final AtomicBoolean active;

    private boolean enableBulkLogging;

    public DefaultBulkController(BulkClient client, BulkMetric bulkMetric) {
        this.client = client;
        this.bulkMetric = bulkMetric;
        this.indexNames = new ArrayList<>();
        this.active = new AtomicBoolean(false);
        this.startBulkRefreshIntervals = new HashMap<>();
        this.stopBulkRefreshIntervals = new HashMap<>();
        this.maxWaitTime = 30L;
        this.maxWaitTimeUnit = TimeUnit.SECONDS;
    }

    @Override
    public Throwable getLastBulkError() {
        return bulkListener.getLastBulkError();
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
        this.enableBulkLogging = settings.getAsBoolean(Parameters.ENABLE_BULK_LOGGING.name(),
                Parameters.ENABLE_BULK_LOGGING.getValue());
        this.bulkListener = new BulkListener();
        this.bulkProcessor = DefaultBulkProcessor.builder(client.getClient(), bulkListener)
                .setBulkActions(maxActionsPerRequest)
                .setConcurrentRequests(maxConcurrentRequests)
                .setFlushInterval(flushIngestInterval)
                .setBulkSize(maxVolumePerRequest)
                .build();
        this.active.set(true);
        if (logger.isInfoEnabled()) {
            logger.info("bulk processor up with maxActionsPerRequest = {} maxConcurrentRequests = {} " +
                            "flushIngestInterval = {} maxVolumePerRequest = {} bulk logging = {} logger debug = {} from settings = {}",
                    maxActionsPerRequest, maxConcurrentRequests, flushIngestInterval, maxVolumePerRequest,
                    enableBulkLogging, logger.isDebugEnabled(), settings.toDelimitedString(','));
        }
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
    public void index(IndexRequest indexRequest) {
        ensureActiveAndBulk();
        if (!active.get()) {
            throw new IllegalStateException("inactive");
        }
        try {
            if (bulkMetric != null) {
                bulkMetric.getCurrentIngest().inc(indexRequest.index(), indexRequest.type(), indexRequest.id());
            }
            bulkProcessor.add(indexRequest);
        } catch (Exception e) {
            bulkListener.lastBulkError = e;
            active.set(false);
            if (logger.isErrorEnabled()) {
                logger.error("bulk add of index failed: " + e.getMessage(), e);
            }
        }
    }

    @Override
    public void delete(DeleteRequest deleteRequest) {
        if (!active.get()) {
            throw new IllegalStateException("inactive");
        }
        try {
            if (bulkMetric != null) {
                bulkMetric.getCurrentIngest().inc(deleteRequest.index(), deleteRequest.type(), deleteRequest.id());
            }
            bulkProcessor.add(deleteRequest);
        } catch (Exception e) {
            bulkListener.lastBulkError = e;
            active.set(false);
            if (logger.isErrorEnabled()) {
                logger.error("bulk add of delete failed: " + e.getMessage(), e);
            }
        }
    }

    @Override
    public void update(UpdateRequest updateRequest) {
        if (!active.get()) {
            throw new IllegalStateException("inactive");
        }
        try {
            if (bulkMetric != null) {
                bulkMetric.getCurrentIngest().inc(updateRequest.index(), updateRequest.type(), updateRequest.id());
            }
            bulkProcessor.add(updateRequest);
        } catch (Exception e) {
            bulkListener.lastBulkError = e;
            active.set(false);
            if (logger.isErrorEnabled()) {
                logger.error("bulk add of update failed: " + e.getMessage(), e);
            }
        }
    }

    @Override
    public boolean waitForResponses(long timeout, TimeUnit timeUnit) {
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
        if (waitForResponses(timeout, timeUnit)) {
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
        if (bulkListener == null) {
            throw new UnsupportedOperationException("bulk listener not present");
        }
    }

    private class BulkListener implements DefaultBulkProcessor.Listener {

        private final Logger logger = LogManager.getLogger(BulkListener.class.getName());

        private Throwable lastBulkError = null;

        @Override
        public void beforeBulk(long executionId, BulkRequest request) {
            long l = 0;
            if (bulkMetric != null) {
                l = bulkMetric.getCurrentIngest().getCount();
                bulkMetric.getCurrentIngest().inc();
                int n = request.numberOfActions();
                bulkMetric.getSubmitted().inc(n);
                bulkMetric.getCurrentIngestNumDocs().inc(n);
                bulkMetric.getTotalIngestSizeInBytes().inc(request.estimatedSizeInBytes());
            }
            if (enableBulkLogging && logger.isDebugEnabled()) {
                logger.debug("before bulk [{}] [actions={}] [bytes={}] [concurrent requests={}]",
                        executionId,
                        request.numberOfActions(),
                        request.estimatedSizeInBytes(),
                        l);
            }
        }

        @Override
        public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
            long l = 0;
            if (bulkMetric != null) {
                l = bulkMetric.getCurrentIngest().getCount();
                bulkMetric.getCurrentIngest().dec();
                bulkMetric.getSucceeded().inc(response.getItems().length);
            }
            int n = 0;
            for (BulkItemResponse itemResponse : response.getItems()) {
                if (bulkMetric != null) {
                    bulkMetric.getCurrentIngest().dec(itemResponse.getIndex(), itemResponse.getType(), itemResponse.getId());
                }
                if (itemResponse.isFailed()) {
                    n++;
                    if (bulkMetric != null) {
                        bulkMetric.getSucceeded().dec(1);
                        bulkMetric.getFailed().inc(1);
                    }
                }
            }
            if (enableBulkLogging && logger.isDebugEnabled() && bulkMetric != null) {
                logger.debug("after bulk [{}] [succeeded={}] [failed={}] [{}ms] {} concurrent requests",
                        executionId,
                        bulkMetric.getSucceeded().getCount(),
                        bulkMetric.getFailed().getCount(),
                        response.getTook().millis(),
                        l);
            }
            if (n > 0) {
                if (enableBulkLogging && logger.isErrorEnabled()) {
                    logger.error("bulk [{}] failed with {} failed items, failure message = {}",
                            executionId, n, response.buildFailureMessage());
                }
            } else {
                if (bulkMetric != null) {
                    bulkMetric.getCurrentIngestNumDocs().dec(response.getItems().length);
                }
            }
        }

        @Override
        public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
            if (bulkMetric != null) {
                bulkMetric.getCurrentIngest().dec();
            }
            lastBulkError = failure;
            active.set(false);
            if (enableBulkLogging && logger.isErrorEnabled()) {
                logger.error("after bulk [" + executionId + "] error", failure);
            }
        }

        Throwable getLastBulkError() {
            return lastBulkError;
        }
    }
}
