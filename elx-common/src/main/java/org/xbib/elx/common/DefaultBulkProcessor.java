package org.xbib.elx.common;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.xbib.elx.api.BulkClient;
import org.xbib.elx.api.BulkMetric;
import org.xbib.elx.api.BulkProcessor;
import org.xbib.elx.api.IndexDefinition;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A bulk processor is a thread safe bulk processing class, allowing to easily
 * set when to "flush" a new bulk request
 * (either based on number of actions, based on the size, or time), and
 * to easily control the number of concurrent bulk
 * requests allowed to be executed in parallel.
 */
public class DefaultBulkProcessor implements BulkProcessor {

    private static final Logger logger = LogManager.getLogger(DefaultBulkProcessor.class);

    private final BulkClient bulkClient;

    private final AtomicBoolean enabled;

    private final ElasticsearchClient client;

    private final DefaultBulkListener bulkListener;

    private final ScheduledThreadPoolExecutor scheduler;

    private ScheduledFuture<?> scheduledFuture;

    private BulkRequest bulkRequest;

    private long maxBulkVolume;

    private int maxBulkActions;

    private final AtomicBoolean closed;

    private final AtomicLong executionIdGen;

    private ResizeableSemaphore semaphore;

    private int permits;

    public DefaultBulkProcessor(BulkClient bulkClient, Settings settings) {
        this.bulkClient = bulkClient;
        int maxActionsPerRequest = settings.getAsInt(Parameters.MAX_ACTIONS_PER_REQUEST.getName(),
                Parameters.MAX_ACTIONS_PER_REQUEST.getInteger());
        int maxConcurrentRequests = settings.getAsInt(Parameters.MAX_CONCURRENT_REQUESTS.getName(),
                Parameters.MAX_CONCURRENT_REQUESTS.getInteger());
        String flushIngestIntervalStr = settings.get(Parameters.FLUSH_INTERVAL.getName(),
                Parameters.FLUSH_INTERVAL.getString());
        TimeValue flushInterval = TimeValue.parseTimeValue(flushIngestIntervalStr,
                TimeValue.timeValueSeconds(30), "");
        ByteSizeValue maxVolumePerRequest = settings.getAsBytesSize(Parameters.MAX_VOLUME_PER_REQUEST.getName(),
                ByteSizeValue.parseBytesSizeValue(Parameters.MAX_VOLUME_PER_REQUEST.getString(), "1m"));
        boolean enableBulkLogging = settings.getAsBoolean(Parameters.ENABLE_BULK_LOGGING.getName(),
                Parameters.ENABLE_BULK_LOGGING.getBoolean());
        boolean failOnBulkError = settings.getAsBoolean(Parameters.FAIL_ON_BULK_ERROR.getName(),
                Parameters.FAIL_ON_BULK_ERROR.getBoolean());
        int ringBufferSize = settings.getAsInt(Parameters.RESPONSE_TIME_COUNT.getName(),
                Parameters.RESPONSE_TIME_COUNT.getInteger());
        this.client = bulkClient.getClient();
        this.scheduler = (ScheduledThreadPoolExecutor) Executors.newScheduledThreadPool(2,
                EsExecutors.daemonThreadFactory("elx-bulk-processor"));
        this.scheduler.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        this.scheduler.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
        if (flushInterval.millis() > 0L) {
            this.scheduledFuture = this.scheduler.scheduleWithFixedDelay(this::flush, flushInterval.millis(),
                    flushInterval.millis(), TimeUnit.MILLISECONDS);
        }
        this.bulkListener = new DefaultBulkListener(this, scheduler,
                enableBulkLogging, failOnBulkError, ringBufferSize);
        this.permits = maxConcurrentRequests;
        this.maxBulkActions = maxActionsPerRequest;
        this.maxBulkVolume = maxVolumePerRequest != null ? maxVolumePerRequest.getBytes() : -1;
        this.bulkRequest = new BulkRequest();
        this.closed = new AtomicBoolean(false);
        this.enabled = new AtomicBoolean(false);
        this.executionIdGen = new AtomicLong();
        if (permits > 0) {
            this.semaphore = new ResizeableSemaphore(permits);
        }
        if (logger.isInfoEnabled()) {
            logger.info("bulk processor now active with maxActionsPerRequest = {} maxConcurrentRequests = {} " +
                            "flushInterval = {} maxVolumePerRequest = {} " +
                            "bulk logging = {} fail on bulk error = {} " +
                            "logger debug = {} from settings = {}",
                    maxActionsPerRequest, maxConcurrentRequests,
                    flushInterval, maxVolumePerRequest,
                    enableBulkLogging, failOnBulkError,
                    logger.isDebugEnabled(), settings.toDelimitedString(','));
        }
        setEnabled(true);
    }

    @Override
    public void setEnabled(boolean enabled) {
        this.enabled.set(enabled);
    }

    @Override
    public void startBulkMode(IndexDefinition indexDefinition) throws IOException {
        String indexName = indexDefinition.getFullIndexName();
        int interval = indexDefinition.getStartBulkRefreshSeconds();
        if (interval != 0) {
            logger.info("starting bulk on " + indexName + " with new refresh interval " + interval);
            bulkClient.updateIndexSetting(indexName, "refresh_interval", interval + "s", 30L, TimeUnit.SECONDS);
        } else {
            logger.warn("ignoring starting bulk on " + indexName + " with refresh interval " + interval);
        }
    }

    @Override
    public void stopBulkMode(IndexDefinition indexDefinition) throws IOException {
        String indexName = indexDefinition.getFullIndexName();
        int interval = indexDefinition.getStopBulkRefreshSeconds();
        flush();
        if (waitForBulkResponses(indexDefinition.getMaxWaitTime(), indexDefinition.getMaxWaitTimeUnit())) {
            if (interval != 0) {
                logger.info("stopping bulk on " + indexName + " with new refresh interval " + interval);
                bulkClient.updateIndexSetting(indexName, "refresh_interval", interval + "s", 30L, TimeUnit.SECONDS);
            } else {
                logger.warn("ignoring stopping bulk on " + indexName + " with refresh interval " + interval);
            }
        }
    }

    @Override
    public void setMaxBulkActions(int bulkActions) {
        this.maxBulkActions = bulkActions;
    }

    @Override
    public int getMaxBulkActions() {
        return maxBulkActions;
    }

    @Override
    public void setMaxBulkVolume(long bulkSize) {
        this.maxBulkVolume = bulkSize;
    }

    @Override
    public long getMaxBulkVolume() {
        return maxBulkVolume;
    }

    @Override
    public BulkMetric getBulkMetric() {
        return bulkListener.getBulkMetric();
    }

    @Override
    public Throwable getLastBulkError() {
        return bulkListener.getLastBulkError();
    }

    @Override
    public synchronized void add(ActionRequest<?> request) {
        ensureOpenAndActive();
        bulkRequest.add(request);
        if ((maxBulkActions != -1 && bulkRequest.numberOfActions() >= maxBulkActions) ||
                (maxBulkVolume != -1 && bulkRequest.estimatedSizeInBytes() >= maxBulkVolume)) {
            execute();
        }
    }

    @Override
    public synchronized void flush() {
        ensureOpenAndActive();
        if (bulkRequest.numberOfActions() > 0) {
            execute();
        }
        // do not drain semaphore
    }

    @Override
    public synchronized boolean waitForBulkResponses(long timeout, TimeUnit unit) {
        try {
            if (closed.get()) {
                // silently skip closed condition
                return true;
            }
            if (bulkRequest.numberOfActions() > 0) {
                execute();
            }
            return drainSemaphore(timeout, unit);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("interrupted while waiting for bulk responses");
            return false;
        }
    }

    @Override
    public synchronized void close() throws IOException {
        if (closed.compareAndSet(false, true)) {
            try {
                if (scheduledFuture != null) {
                    scheduledFuture.cancel(true);
                }
                if (scheduler != null) {
                    scheduler.shutdown();
                }
                // like flush but without ensuring open
                if (bulkRequest.numberOfActions() > 0) {
                    execute();
                }
                drainSemaphore(0L, TimeUnit.NANOSECONDS);
                bulkListener.close();
            } catch (InterruptedException exc) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void execute() {
        BulkRequest myBulkRequest = this.bulkRequest;
        this.bulkRequest = new BulkRequest();
        long executionId = executionIdGen.incrementAndGet();
        if (semaphore == null) {
            boolean afterCalled = false;
            try {
                bulkListener.beforeBulk(executionId, myBulkRequest);
                BulkResponse bulkResponse = client.execute(BulkAction.INSTANCE, myBulkRequest).actionGet();
                afterCalled = true;
                bulkListener.afterBulk(executionId, myBulkRequest, bulkResponse);
            } catch (Exception e) {
                if (!afterCalled) {
                    bulkListener.afterBulk(executionId, myBulkRequest, e);
                }
            }
        } else {
            boolean bulkRequestSetupSuccessful = false;
            boolean acquired = false;
            try {
                bulkListener.beforeBulk(executionId, myBulkRequest);
                semaphore.acquire();
                acquired = true;
                client.execute(BulkAction.INSTANCE, myBulkRequest, new ActionListener<>() {
                    @Override
                    public void onResponse(BulkResponse response) {
                        try {
                            bulkListener.afterBulk(executionId, myBulkRequest, response);
                        } finally {
                            semaphore.release();
                        }
                    }

                    @Override
                    public void onFailure(Throwable e) {
                        try {
                            bulkListener.afterBulk(executionId, myBulkRequest, e);
                        } finally {
                            semaphore.release();
                        }
                    }
                });
                bulkRequestSetupSuccessful = true;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                bulkListener.afterBulk(executionId, myBulkRequest, e);
            } catch (Exception e) {
                bulkListener.afterBulk(executionId, myBulkRequest, e);
            } finally {
                if (!bulkRequestSetupSuccessful && acquired) {
                    semaphore.release();
                }
            }
        }
    }

    private boolean drainSemaphore(long timeValue, TimeUnit timeUnit) throws InterruptedException {
        if (semaphore != null) {
            if (semaphore.tryAcquire(permits, timeValue, timeUnit)) {
                semaphore.release(permits);
                return true;
            }
        }
        return false;
    }

    private void ensureOpenAndActive() {
        if (closed.get()) {
            throw new IllegalStateException("bulk processor is closed");
        }
        if (!enabled.get()) {
            throw new IllegalStateException("bulk processor is no longer enabled");
        }
    }

    private static class ResizeableSemaphore extends Semaphore {

        ResizeableSemaphore(int permits) {
            super(permits, true);
        }

        @Override
        protected void reducePermits(int reduction) {
            super.reducePermits(reduction);
        }
    }
}
