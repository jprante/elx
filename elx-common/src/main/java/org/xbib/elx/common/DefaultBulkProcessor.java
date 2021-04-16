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
import org.xbib.elx.api.BulkListener;
import org.xbib.elx.api.BulkProcessor;
import org.xbib.elx.api.BulkRequestHandler;

import java.io.IOException;
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

    private final ScheduledThreadPoolExecutor scheduler;

    private final ScheduledFuture<?> scheduledFuture;

    private final BulkRequestHandler bulkRequestHandler;

    private BulkRequest bulkRequest;

    private long bulkSize;

    private int bulkActions;

    private volatile boolean closed;

    private DefaultBulkProcessor(ElasticsearchClient client,
                                 BulkListener bulkListener,
                                 String name,
                                 int concurrentRequests,
                                 int bulkActions,
                                 ByteSizeValue bulkSize,
                                 TimeValue flushInterval) {
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
    public void setBulkActions(int bulkActions) {
        this.bulkActions = bulkActions;
    }

    @Override
    public int getBulkActions() {
        return bulkActions;
    }

    @Override
    public void setBulkSize(long bulkSize) {
        this.bulkSize = bulkSize;
    }

    @Override
    public long getBulkSize() {
        return bulkSize;
    }

    public BulkRequestHandler getBulkRequestHandler() {
        return bulkRequestHandler;
    }

    @Override
    public synchronized void add(ActionRequest<?> request) {
        ensureOpen();
        bulkRequest.add(request);
        if ((bulkActions != -1 && bulkRequest.numberOfActions() >= bulkActions) ||
                (bulkSize != -1 && bulkRequest.estimatedSizeInBytes() >= bulkSize)) {
            execute();
        }
    }

    @Override
    public synchronized void flush() {
        ensureOpen();
        if (bulkRequest.numberOfActions() > 0) {
            execute();
        }
    }

    @Override
    public synchronized boolean awaitFlush(long timeout, TimeUnit unit) throws InterruptedException, IOException {
        if (closed) {
            return true;
        }
        if (bulkRequest.numberOfActions() > 0) {
            execute();
        }
        return bulkRequestHandler.flush(timeout, unit);
    }

    @Override
    public synchronized void close() throws IOException {
        try {
            if (closed) {
                return;
            }
            closed = true;
            if (scheduledFuture != null) {
                scheduledFuture.cancel(true);
                scheduler.shutdown();
            }
            if (bulkRequest.numberOfActions() > 0) {
                execute();
            }
            bulkRequestHandler.flush(0, TimeUnit.NANOSECONDS);
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
        this.bulkRequest = new BulkRequest();
        this.bulkRequestHandler.execute(myBulkRequest);
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
         * {@code 1mb}. Can be set to {@code -1} to disable it.
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
                if (bulkRequest.numberOfActions() > 0) {
                    execute();
                }
            }
        }
    }

    private static class SyncBulkRequestHandler implements BulkRequestHandler {

        private final ElasticsearchClient client;

        private final BulkListener bulkListener;

        private final AtomicLong executionIdGen;

        SyncBulkRequestHandler(ElasticsearchClient client, BulkListener bulkListener) {
            this.client = client;
            this.bulkListener = bulkListener;
            this.executionIdGen = new AtomicLong();
        }

        @Override
        public void execute(BulkRequest bulkRequest) {
            long executionId = executionIdGen.incrementAndGet();
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
        public boolean flush(long timeout, TimeUnit unit) {
            return true;
        }

        @Override
        public int getPermits() {
            return 1;
        }

        @Override
        public void increase() {
        }

        @Override
        public void reduce() {
        }
    }

    private static class AsyncBulkRequestHandler implements BulkRequestHandler {

        private final ElasticsearchClient client;

        private final BulkListener bulkListener;

        private final ResizeableSemaphore semaphore;

        private final AtomicLong executionIdGen;

        private int permits;

        private AsyncBulkRequestHandler(ElasticsearchClient client,
                                        BulkListener bulkListener,
                                        int permits) {
            this.client = client;
            this.bulkListener = bulkListener;
            this.permits = permits;
            this.semaphore = new ResizeableSemaphore(permits);
            this.executionIdGen = new AtomicLong();
        }

        @Override
        public void execute(BulkRequest bulkRequest) {
            boolean bulkRequestSetupSuccessful = false;
            boolean acquired = false;
            long executionId = executionIdGen.incrementAndGet();
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
                    semaphore.release();
                }
            }
        }

        @Override
        public boolean flush(long timeout, TimeUnit unit) throws IOException, InterruptedException {
            bulkListener.close();
            if (semaphore.tryAcquire(permits, timeout, unit)) {
                semaphore.release(permits);
                return true;
            }
            return false;
        }

        @Override
        public int getPermits() {
            return permits;
        }

        @Override
        public void increase() {
            semaphore.release(1);
            this.permits++;
        }

        @Override
        public void reduce() {
            semaphore.reducePermits(1);
            this.permits--;
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
