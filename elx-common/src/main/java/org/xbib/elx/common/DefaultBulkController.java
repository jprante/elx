package org.xbib.elx.common;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.xbib.elx.api.BulkClient;
import org.xbib.elx.api.BulkController;
import org.xbib.elx.api.BulkListener;
import org.xbib.elx.api.BulkMetric;
import org.xbib.elx.api.BulkProcessor;
import org.xbib.elx.api.IndexDefinition;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class DefaultBulkController implements BulkController {

    private static final Logger logger = LogManager.getLogger(DefaultBulkController.class);

    private final BulkClient bulkClient;

    private final BulkMetric bulkMetric;

    private BulkProcessor bulkProcessor;

    private BulkListener bulkListener;

    private long maxWaitTime;

    private TimeUnit maxWaitTimeUnit;

    private final AtomicBoolean active;

    public DefaultBulkController(BulkClient bulkClient) {
        this.bulkClient = bulkClient;
        this.bulkMetric = new DefaultBulkMetric();
        this.active = new AtomicBoolean(false);
    }

    @Override
    public void init(Settings settings) {
        bulkMetric.init(settings);
        String maxWaitTimeStr = settings.get(Parameters.MAX_WAIT_BULK_RESPONSE.getName(),
                 Parameters.MAX_WAIT_BULK_RESPONSE.getString());
        TimeValue maxWaitTimeValue = TimeValue.parseTimeValue(maxWaitTimeStr,
                TimeValue.timeValueSeconds(30), "");
        this.maxWaitTime = maxWaitTimeValue.seconds();
        this.maxWaitTimeUnit = TimeUnit.SECONDS;
        int maxActionsPerRequest = settings.getAsInt(Parameters.MAX_ACTIONS_PER_REQUEST.getName(),
                Parameters.MAX_ACTIONS_PER_REQUEST.getInteger());
        int maxConcurrentRequests = settings.getAsInt(Parameters.MAX_CONCURRENT_REQUESTS.getName(),
                Parameters.MAX_CONCURRENT_REQUESTS.getInteger());
        String flushIngestIntervalStr = settings.get(Parameters.FLUSH_INTERVAL.getName(),
                Parameters.FLUSH_INTERVAL.getString());
        TimeValue flushIngestInterval = TimeValue.parseTimeValue(flushIngestIntervalStr,
                TimeValue.timeValueSeconds(30), "");
        ByteSizeValue maxVolumePerRequest = settings.getAsBytesSize(Parameters.MAX_VOLUME_PER_REQUEST.getName(),
                ByteSizeValue.parseBytesSizeValue(Parameters.MAX_VOLUME_PER_REQUEST.getString(), "1m"));
        boolean enableBulkLogging = settings.getAsBoolean(Parameters.ENABLE_BULK_LOGGING.getName(),
                Parameters.ENABLE_BULK_LOGGING.getBoolean());
        boolean failOnBulkError = settings.getAsBoolean(Parameters.FAIL_ON_BULK_ERROR.getName(),
                Parameters.FAIL_ON_BULK_ERROR.getBoolean());
        int responseTimeCount = settings.getAsInt(Parameters.RESPONSE_TIME_COUNT.getName(),
                Parameters.RESPONSE_TIME_COUNT.getInteger());
        this.bulkListener = new DefaultBulkListener(this, bulkMetric,
                enableBulkLogging, failOnBulkError, responseTimeCount);
        this.bulkProcessor = DefaultBulkProcessor.builder(bulkClient.getClient(), bulkListener)
                .setBulkActions(maxActionsPerRequest)
                .setConcurrentRequests(maxConcurrentRequests)
                .setFlushInterval(flushIngestInterval)
                .setBulkSize(maxVolumePerRequest)
                .build();
        this.active.set(true);
        if (logger.isInfoEnabled()) {
            logger.info("bulk processor now active with maxWaitTime = {} maxActionsPerRequest = {} maxConcurrentRequests = {} " +
                            "flushIngestInterval = {} maxVolumePerRequest = {} " +
                            "bulk logging = {} fail on bulk error = {} " +
                            "logger debug = {} from settings = {}",
                    maxWaitTimeStr, maxActionsPerRequest, maxConcurrentRequests,
                    flushIngestInterval, maxVolumePerRequest,
                    enableBulkLogging, failOnBulkError,
                    logger.isDebugEnabled(), settings.toDelimitedString(','));
        }
    }

    @Override
    public BulkProcessor getBulkProcessor() {
        return bulkProcessor;
    }

    @Override
    public BulkMetric getBulkMetric() {
        return bulkMetric;
    }

    @Override
    public Throwable getLastBulkError() {
        return bulkListener != null ? bulkListener.getLastBulkError() : null;
    }

    @Override
    public void inactivate() {
        this.active.set(false);
    }

    @Override
    public void startBulkMode(IndexDefinition indexDefinition) throws IOException {
        String indexName = indexDefinition.getFullIndexName();
        if (indexDefinition.getStartBulkRefreshSeconds() != 0) {
            bulkClient.updateIndexSetting(indexName, "refresh_interval",
                    indexDefinition.getStartBulkRefreshSeconds() + "s",
                    30L, TimeUnit.SECONDS);
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
        ensureActiveAndBulk();
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
        ensureActiveAndBulk();
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
            if (bulkProcessor != null) {
                bulkProcessor.flush();
                return bulkProcessor.awaitFlush(timeout, timeUnit);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("interrupted");
            return false;
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
            return false;
        }
        return false;
    }

    @Override
    public void stopBulkMode(IndexDefinition indexDefinition) throws IOException {
        flush();
        if (waitForBulkResponses(indexDefinition.getMaxWaitTime(), indexDefinition.getMaxWaitTimeUnit())) {
            if (indexDefinition.getStopBulkRefreshSeconds() != 0) {
                bulkClient.updateIndexSetting(indexDefinition.getFullIndexName(),
                        "refresh_interval",
                        indexDefinition.getStopBulkRefreshSeconds() + "s",
                        30L, TimeUnit.SECONDS);
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
        bulkMetric.close();
        flush();
        bulkClient.waitForResponses(maxWaitTime, maxWaitTimeUnit);
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
