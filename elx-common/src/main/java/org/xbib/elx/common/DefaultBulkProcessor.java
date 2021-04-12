package org.xbib.elx.common;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.xbib.elx.api.BulkListener;
import org.xbib.elx.api.BulkProcessor;
import org.xbib.elx.api.BulkRequestHandler;

import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A bulk processor is a thread safe bulk processing class, allowing to easily
 * set when to "flush" a new bulk request
 * (either based on number of actions, based on the size, or time), and
 * to easily control the number of concurrent bulk
 * requests allowed to be executed in parallel.
 * In order to create a new bulk processor, use the {@link Builder}.
 */
public class DefaultBulkProcessor implements BulkProcessor {

    private final BulkListener bulkListener;

    private final int bulkActions;

    private final long bulkSize;

    private final ScheduledThreadPoolExecutor scheduler;

    private final ScheduledFuture<?> scheduledFuture;

    private final AtomicLong executionIdGen;

    private final BulkRequestHandler bulkRequestHandler;

    private BulkRequest bulkRequest;

    private volatile boolean closed;

    private DefaultBulkProcessor(ElasticsearchClient client,
                                 BulkListener bulkListener,
                                 String name,
                                 int concurrentRequests,
                                 int bulkActions,
                                 ByteSizeValue bulkSize,
                                 TimeValue flushInterval) {
        this.bulkListener = bulkListener;
        this.executionIdGen = new AtomicLong();
        this.closed = false;
        this.bulkActions = bulkActions;
        this.bulkSize = bulkSize.getBytes();
        this.bulkRequest = new BulkRequest();
        this.bulkRequestHandler = concurrentRequests == 0 ?
                new SyncBulkRequestHandler(client, bulkListener) :
                new AsyncBulkRequestHandler(client, bulkListener, concurrentRequests);
        if (flushInterval != null) {
            this.scheduler = (ScheduledThreadPoolExecutor) Executors.newScheduledThreadPool(1,
                    EsExecutors.daemonThreadFactory(name != null ? "[" + name + "]" : "" + "bulk_processor"));
            this.scheduler.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
            this.scheduler.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
            this.scheduledFuture = this.scheduler.scheduleWithFixedDelay(new Flush(), flushInterval.millis(),
                    flushInterval.millis(), TimeUnit.MILLISECONDS);
        } else {
            this.scheduler = null;
            this.scheduledFuture = null;
        }
    }

    public static Builder builder(ElasticsearchClient client, BulkListener bulkListener) {
        Objects.requireNonNull(bulkListener, "A listener for the BulkProcessor is required but null");
        return new Builder(client, bulkListener);
    }

    @Override
    public BulkListener getBulkListener() {
        return bulkListener;
    }

    /**
     * Wait for bulk request handler with flush.
     * @param timeout the timeout value
     * @param unit the timeout unit
     * @return true is method was successful, false if timeout
     * @throws InterruptedException if timeout
     */
    @Override
    public synchronized boolean awaitFlush(long timeout, TimeUnit unit) throws InterruptedException {
        Objects.requireNonNull(unit, "A time unit is required for awaitFlush() but null");
        if (closed) {
            return true;
        }
        // flush
        if (bulkRequest.numberOfActions() > 0) {
            execute();
        }
        // wait for all bulk responses
        return bulkRequestHandler.close(timeout, unit);
    }

    /**
     * Closes the processor. Any remaining bulk actions are flushed and then closed. This emthod can only be called
     * once as the last action of a bulk processor.
     *
     * If concurrent requests are not enabled, returns {@code true} immediately.
     * If concurrent requests are enabled, waits for up to the specified timeout for all bulk requests to complete then
     * returns {@code true},
     * If the specified waiting time elapses before all bulk requests complete, {@code false} is returned.
     *
     * @param timeout The maximum time to wait for the bulk requests to complete
     * @param unit    The time unit of the {@code timeout} argument
     * @return {@code true} if all bulk requests completed and {@code false} if the waiting time elapsed before all the
     * bulk requests completed
     * @throws InterruptedException If the current thread is interrupted
     */
    @Override
    public synchronized boolean awaitClose(long timeout, TimeUnit unit) throws InterruptedException {
        Objects.requireNonNull(unit, "A time unit is required for awaitCLose() but null");
        if (closed) {
            return true;
        }
        closed = true;
        if (scheduledFuture != null) {
            FutureUtils.cancel(scheduledFuture);
            scheduler.shutdown();
        }
        if (bulkRequest.numberOfActions() > 0) {
            execute();
        }
        return bulkRequestHandler.close(timeout, unit);
    }

    /**
     * Adds either a delete or an index request.
     *
     * @param request request
     * @return his bulk processor
     */
    @Override
    public synchronized DefaultBulkProcessor add(ActionRequest<?> request) {
        ensureOpen();
        bulkRequest.add(request);
        if ((bulkActions != -1 && bulkRequest.numberOfActions() >= bulkActions) ||
                (bulkSize != -1 && bulkRequest.estimatedSizeInBytes() >= bulkSize)) {
            execute();
        }
        return this;
    }

    /**
     * Flush pending delete or index requests.
     */
    @Override
    public synchronized void flush() {
        ensureOpen();
        if (bulkRequest.numberOfActions() > 0) {
            execute();
        }
    }

    /**
     * Closes the processor. If flushing by time is enabled, then it's shutdown. Any remaining bulk actions are flushed.
     */
    @Override
    public void close() {
        try {
            // 0 = immediate close
            awaitClose(0, TimeUnit.NANOSECONDS);
        } catch (InterruptedException exc) {
            Thread.currentThread().interrupt();
        }
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("bulk processor already closed");
        }
    }

    private void execute() {
        BulkRequest myBulkRequest = this.bulkRequest;
        long executionId = executionIdGen.incrementAndGet();
        this.bulkRequest = new BulkRequest();
        this.bulkRequestHandler.execute(myBulkRequest, executionId);
    }

    /**
     * A builder used to create a build an instance of a bulk processor.
     */
    public static class Builder {

        private final ElasticsearchClient client;

        private final BulkListener bulkListener;

        private String name;

        private int concurrentRequests = 1;

        private int bulkActions = 1000;

        private ByteSizeValue bulkSize = new ByteSizeValue(10, ByteSizeUnit.MB);

        private TimeValue flushInterval = null;

        /**
         * Creates a builder of bulk processor with the client to use and the listener that will be used
         * to be notified on the completion of bulk requests.
         *
         * @param client the client
         * @param bulkListener the listener
         */
        Builder(ElasticsearchClient client, BulkListener bulkListener) {
            this.client = client;
            this.bulkListener = bulkListener;
        }

        /**
         * Sets an optional name to identify this bulk processor.
         *
         * @param name name
         * @return this builder
         */
        public Builder setName(String name) {
            this.name = name;
            return this;
        }

        /**
         * Sets the number of concurrent requests allowed to be executed. A value of 0 means that only a single
         * request will be allowed to be executed. A value of 1 means 1 concurrent request is allowed to be executed
         * while accumulating new bulk requests. Defaults to {@code 1}.
         *
         * @param concurrentRequests maximum number of concurrent requests
         * @return this builder
         */
        public Builder setConcurrentRequests(int concurrentRequests) {
            this.concurrentRequests = concurrentRequests;
            return this;
        }

        /**
         * Sets when to flush a new bulk request based on the number of actions currently added. Defaults to
         * {@code 1000}. Can be set to {@code -1} to disable it.
         *
         * @param bulkActions bulk actions
         * @return this builder
         */
        public Builder setBulkActions(int bulkActions) {
            this.bulkActions = bulkActions;
            return this;
        }

        /**
         * Sets when to flush a new bulk request based on the size of actions currently added. Defaults to
         * {@code 5mb}. Can be set to {@code -1} to disable it.
         *
         * @param bulkSize bulk size
         * @return this builder
         */
        public Builder setBulkSize(ByteSizeValue bulkSize) {
            this.bulkSize = bulkSize;
            return this;
        }

        /**
         * Sets a flush interval flushing *any* bulk actions pending if the interval passes. Defaults to not set.
         * Note, both {@link #setBulkActions(int)} and {@link #setBulkSize(org.elasticsearch.common.unit.ByteSizeValue)}
         * can be set to {@code -1} with the flush interval set allowing for complete async processing of bulk actions.
         *
         * @param flushInterval flush interval
         * @return this builder
         */
        public Builder setFlushInterval(TimeValue flushInterval) {
            this.flushInterval = flushInterval;
            return this;
        }

        /**
         * Builds a new bulk processor.
         *
         * @return a bulk processor
         */
        public DefaultBulkProcessor build() {
            return new DefaultBulkProcessor(client, bulkListener, name, concurrentRequests, bulkActions, bulkSize, flushInterval);
        }
    }

    private class Flush implements Runnable {

        @Override
        public void run() {
            synchronized (DefaultBulkProcessor.this) {
                if (closed) {
                    return;
                }
                if (bulkRequest.numberOfActions() == 0) {
                    return;
                }
                execute();
            }
        }
    }

    private static class SyncBulkRequestHandler implements BulkRequestHandler {

        private final ElasticsearchClient client;

        private final BulkListener bulkListener;

        SyncBulkRequestHandler(ElasticsearchClient client, BulkListener bulkListener) {
            Objects.requireNonNull(bulkListener, "A listener is required for SyncBulkRequestHandler but null");
            this.client = client;
            this.bulkListener = bulkListener;
        }

        @Override
        public void execute(BulkRequest bulkRequest, long executionId) {
            boolean afterCalled = false;
            try {
                bulkListener.beforeBulk(executionId, bulkRequest);
                BulkResponse bulkResponse = client.execute(BulkAction.INSTANCE, bulkRequest).actionGet();
                afterCalled = true;
                bulkListener.afterBulk(executionId, bulkRequest, bulkResponse);
            } catch (Exception e) {
                if (!afterCalled) {
                    bulkListener.afterBulk(executionId, bulkRequest, e);
                }
            }
        }

        @Override
        public boolean close(long timeout, TimeUnit unit) {
            return true;
        }
    }

    private static class AsyncBulkRequestHandler implements BulkRequestHandler {

        private final ElasticsearchClient client;

        private final BulkListener bulkListener;

        private final Semaphore semaphore;

        private final int concurrentRequests;

        private AsyncBulkRequestHandler(ElasticsearchClient client, BulkListener bulkListener, int concurrentRequests) {
            Objects.requireNonNull(bulkListener, "A listener is required for AsyncBulkRequestHandler but null");
            this.client = client;
            this.bulkListener = bulkListener;
            this.concurrentRequests = concurrentRequests;
            this.semaphore = new Semaphore(concurrentRequests);
        }

        @Override
        public void execute(final BulkRequest bulkRequest, final long executionId) {
            boolean bulkRequestSetupSuccessful = false;
            boolean acquired = false;
            try {
                bulkListener.beforeBulk(executionId, bulkRequest);
                semaphore.acquire();
                acquired = true;
                client.execute(BulkAction.INSTANCE, bulkRequest, new ActionListener<>() {
                    @Override
                    public void onResponse(BulkResponse response) {
                        try {
                            bulkListener.afterBulk(executionId, bulkRequest, response);
                        } finally {
                            semaphore.release();
                        }
                    }

                    @Override
                    public void onFailure(Throwable e) {
                        try {
                            bulkListener.afterBulk(executionId, bulkRequest, e);
                        } finally {
                            semaphore.release();
                        }
                    }
                });
                bulkRequestSetupSuccessful = true;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                bulkListener.afterBulk(executionId, bulkRequest, e);
            } catch (Exception e) {
                bulkListener.afterBulk(executionId, bulkRequest, e);
            } finally {
                if (!bulkRequestSetupSuccessful && acquired) {
                    // if we fail on client.bulk() release the semaphore
                    semaphore.release();
                }
            }
        }

        @Override
        public boolean close(long timeout, TimeUnit unit) throws InterruptedException {
            if (semaphore.tryAcquire(concurrentRequests, timeout, unit)) {
                semaphore.release(concurrentRequests);
                return true;
            }
            return false;
        }
    }
}
