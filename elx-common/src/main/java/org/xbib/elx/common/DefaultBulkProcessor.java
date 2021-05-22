package org.xbib.elx.common;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.xbib.elx.api.BulkClient;
import org.xbib.elx.api.BulkMetric;
import org.xbib.elx.api.BulkProcessor;

import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
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

    private final AtomicBoolean enabled;

    private final BulkClient bulkClient;

    private final ElasticsearchClient client;

    private final DefaultBulkListener bulkListener;

    private ScheduledFuture<?> flushIntervalFuture;

    private BulkRequest bulkRequest;

    private long bulkVolume;

    private int bulkActions;

    private final AtomicBoolean closed;

    private final AtomicLong executionIdGen;

    private final ResizeableSemaphore semaphore;

    private final int permits;

    public DefaultBulkProcessor(BulkClient bulkClient, Settings settings) {
        this.bulkClient = bulkClient;
        this.closed = new AtomicBoolean(false);
        this.enabled = new AtomicBoolean(false);
        int maxActionsPerRequest = settings.getAsInt(Parameters.BULK_MAX_ACTIONS_PER_REQUEST.getName(),
                Parameters.BULK_MAX_ACTIONS_PER_REQUEST.getInteger());
        String flushIntervalStr = settings.get(Parameters.BULK_FLUSH_INTERVAL.getName(),
                Parameters.BULK_FLUSH_INTERVAL.getString());
        TimeValue flushInterval = TimeValue.parseTimeValue(flushIntervalStr,
                TimeValue.timeValueSeconds(30), "");
        this.client = bulkClient.getClient();
        if (flushInterval.millis() > 0L) {
            this.flushIntervalFuture = bulkClient.getScheduler().scheduleWithFixedDelay(this::flush, flushInterval.millis(),
                    flushInterval.millis(), TimeUnit.MILLISECONDS);
        }
        this.bulkListener = new DefaultBulkListener(this, settings);
        this.bulkActions = maxActionsPerRequest;
        ByteSizeValue minVolumePerRequest = settings.getAsBytesSize(Parameters.BULK_MIN_VOLUME_PER_REQUEST.getName(),
                ByteSizeValue.parseBytesSizeValue(Parameters.BULK_MIN_VOLUME_PER_REQUEST.getString(), "1k"));
        this.bulkVolume = minVolumePerRequest.getBytes();
        if (!isBulkMetricEnabled()) {
            ByteSizeValue maxVolumePerRequest = settings.getAsBytesSize(Parameters.BULK_MAX_VOLUME_PER_REQUEST.getName(),
                    ByteSizeValue.parseBytesSizeValue(Parameters.BULK_MAX_VOLUME_PER_REQUEST.getString(), "1m"));
            this.bulkVolume = maxVolumePerRequest.getBytes();
        }
        this.bulkRequest = new BulkRequest();
        this.executionIdGen = new AtomicLong();
        this.permits = settings.getAsInt(Parameters.BULK_PERMITS.getName(), Parameters.BULK_PERMITS.getInteger());
        if (permits < 1) {
            throw new IllegalArgumentException("must not be less 1 permits for bulk indexing");
        }
        this.semaphore = new ResizeableSemaphore(permits);
        logger.info("bulk processor now enabled");
        setEnabled(true);
    }

    @Override
    public void setEnabled(boolean enabled) {
        this.enabled.set(enabled);
    }

    @Override
    public void setMaxBulkActions(int bulkActions) {
        this.bulkActions = bulkActions;
    }

    @Override
    public int getMaxBulkActions() {
        return bulkActions;
    }

    @Override
    public void setMaxBulkVolume(long bulkVolume) {
        this.bulkVolume = bulkVolume;
    }

    @Override
    public long getMaxBulkVolume() {
        return bulkVolume;
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public ScheduledExecutorService getScheduler() {
        return bulkClient.getScheduler();
    }

    @Override
    public BulkMetric getBulkMetric() {
        return bulkListener.getBulkMetric();
    }

    @Override
    public boolean isBulkMetricEnabled() {
        return bulkListener.getBulkMetric() != null;
    }

    @Override
    public Throwable getLastBulkError() {
        return bulkListener.getLastBulkError();
    }

    @Override
    public synchronized void add(DocWriteRequest<?> request) {
        ensureOpenAndActive();
        bulkRequest.add(request);
        if ((bulkActions != -1 && bulkRequest.numberOfActions() >= bulkActions) ||
                (bulkVolume != -1 && bulkRequest.estimatedSizeInBytes() >= bulkVolume)) {
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

        } catch (InterruptedException exc) {
            Thread.currentThread().interrupt();
            logger.error("interrupted while waiting for bulk responses");
            return false;
        }
    }

    @Override
    public synchronized void close() throws IOException {
        if (closed.compareAndSet(false, true)) {
            try {
                if (flushIntervalFuture != null) {
                    flushIntervalFuture.cancel(true);
                }
                // like flush but without ensuring open
                if (bulkRequest.numberOfActions() > 0) {
                    execute();
                }
                drainSemaphore(0L, TimeUnit.NANOSECONDS);
                bulkListener.close();
            } catch (InterruptedException e) {
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
                    public void onFailure(Exception e) {
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
            if (permits <= 0) {
                return true;
            } else {
                if (semaphore.tryAcquire(permits, timeValue, timeUnit)) {
                    semaphore.release(permits);
                    return true;
                }
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

    @SuppressWarnings("serial")
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
