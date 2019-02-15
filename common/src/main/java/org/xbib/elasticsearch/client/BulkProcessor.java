package org.xbib.elasticsearch.client;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;

import java.io.Closeable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A bulk processor is a thread safe bulk processing class, allowing to easily set when to "flush" a new bulk request
 * (either based on number of actions, based on the size, or time), and to easily control the number of concurrent bulk
 * requests allowed to be executed in parallel.
 * In order to create a new bulk processor, use the {@link Builder}.
 */
public class BulkProcessor implements Closeable {

    private final int maximumBulkActionsPerRequest;

    private final long maximumBulkRequestByteSize;

    private final ScheduledThreadPoolExecutor scheduler;

    private final ScheduledFuture<?> scheduledFuture;

    private final AtomicLong executionIdGen = new AtomicLong();

    private final BulkExecutor bulkExecutor;

    private BulkRequest bulkRequest;

    private volatile boolean closed = false;

    private BulkProcessor(ElasticsearchClient client, Listener listener, int maximumConcurrentBulkRequests,
                          int maximumBulkActionsPerRequest, ByteSizeValue maximumBulkRequestByteSize,
                          @Nullable TimeValue flushInterval) {
        this.maximumBulkActionsPerRequest = maximumBulkActionsPerRequest;
        this.maximumBulkRequestByteSize = maximumBulkRequestByteSize.getBytes();
        this.bulkRequest = new BulkRequest();
        this.bulkExecutor = maximumConcurrentBulkRequests == 0 ?
                new SyncBulkExecutor(client, listener) :
                new AsyncBulkExecutor(client, listener, maximumConcurrentBulkRequests);

        if (flushInterval != null) {
            this.scheduler = (ScheduledThreadPoolExecutor) Executors.newScheduledThreadPool(1);
            this.scheduler.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
            this.scheduler.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
            this.scheduledFuture = this.scheduler.scheduleWithFixedDelay(new Flush(), flushInterval.millis(),
                    flushInterval.millis(), TimeUnit.MILLISECONDS);
        } else {
            this.scheduler = null;
            this.scheduledFuture = null;
        }
    }

    public static Builder builder(ElasticsearchClient client, Listener listener) {
        if (client == null) {
            throw new NullPointerException("The client you specified while building a BulkProcessor is null");
        }
        return new Builder(client, listener);
    }

    /**
     * Closes the processor. If flushing by time is enabled, then it's shutdown. Any remaining bulk actions are flushed.
     */
    @Override
    public void close() {
        try {
            awaitClose(0, TimeUnit.NANOSECONDS);
        } catch (InterruptedException exc) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Closes the processor. If flushing by time is enabled, then it's shutdown. Any remaining bulk actions are
     * flushed.
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
    public synchronized boolean awaitClose(long timeout, TimeUnit unit) throws InterruptedException {
        if (closed) {
            return true;
        }
        closed = true;
        if (this.scheduledFuture != null) {
            this.scheduledFuture.cancel(false);
            this.scheduler.shutdown();
        }
        if (bulkRequest.numberOfActions() > 0) {
            execute();
        }
        return bulkExecutor.awaitClose(timeout, unit);
    }

    /**
     * Adds an {@link IndexRequest} to the list of actions to execute. Follows the same behavior of {@link IndexRequest}
     * (for example, if no id is provided, one will be generated, or usage of the create flag).
     *
     * @param request request
     * @return his bulk processor
     */
    public synchronized BulkProcessor add(IndexRequest request) {
        if (request == null) {
            return this;
        }
        ensureOpen();
        bulkRequest.add(request);
        if (isOverTheLimit()) {
            execute();
        }
        return this;
    }

    /**
     * Adds an {@link DeleteRequest} to the list of actions to execute.
     *
     * @param request request
     * @return his bulk processor
     */
    public synchronized BulkProcessor add(DeleteRequest request) {
        if (request == null) {
            return this;
        }
        ensureOpen();
        bulkRequest.add(request);
        if (isOverTheLimit()) {
            execute();
        }
        return this;
    }

    /**
     * Adds an {@link UpdateRequest} to the list of actions to execute.
     *
     * @param request request
     * @return his bulk processor
     */
    public synchronized BulkProcessor add(UpdateRequest request) {
        if (request == null) {
            return this;
        }
        ensureOpen();
        bulkRequest.add(request);
        if (isOverTheLimit()) {
            execute();
        }
        return this;
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("bulk process already closed");
        }
    }

    private boolean isOverTheLimit() {
        final int count = bulkRequest.numberOfActions();
        return count > 0 &&
                (maximumBulkActionsPerRequest != -1 && count >= maximumBulkActionsPerRequest) ||
                (maximumBulkRequestByteSize != -1 && bulkRequest.estimatedSizeInBytes() >= maximumBulkRequestByteSize);
    }

    private void execute() {
        final BulkRequest myBulkRequest = this.bulkRequest;
        bulkExecutor.execute(myBulkRequest, executionIdGen.incrementAndGet());
        this.bulkRequest = new BulkRequest();
    }

    /**
     * Flush pending delete or index requests.
     */
    public synchronized void flush() {
        ensureOpen();
        if (bulkRequest.numberOfActions() > 0) {
            execute();
        }
    }

    /**
     * A listener for the execution.
     */
    public interface Listener {

        /**
         * Callback before the bulk is executed.
         *
         * @param executionId execution ID
         * @param request     request
         */
        void beforeBulk(long executionId, BulkRequest request);

        /**
         * Callback after a successful execution of bulk request.
         *
         * @param executionId execution ID
         * @param request     request
         * @param response    response
         */
        void afterBulk(long executionId, BulkRequest request, BulkResponse response);

        /**
         * Callback after a failed execution of bulk request.
         *
         * Note that in case an instance of <code>InterruptedException</code> is passed, which means that request
         * processing has been
         * cancelled externally, the thread's interruption status has been restored prior to calling this method.
         *
         * @param executionId execution ID
         * @param request     request
         * @param failure     failure
         */
        void afterBulk(long executionId, BulkRequest request, Throwable failure);
    }

    /**
     * A builder used to create a build an instance of a bulk processor.
     */
    public static class Builder {

        private final ElasticsearchClient client;
        private final Listener listener;
        private int concurrentRequests = 1;
        private int bulkActions = 1000;
        private ByteSizeValue bulkSize = new ByteSizeValue(5, ByteSizeUnit.MB);
        private TimeValue flushInterval = null;

        /**
         * Creates a builder of bulk processor with the client to use and the listener that will be used
         * to be notified on the completion of bulk requests.
         *
         * @param client   the client
         * @param listener the listener
         */
        Builder(ElasticsearchClient client, Listener listener) {
            this.client = client;
            this.listener = listener;
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
         * @param bulkActions mbulk actions
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
         * Note, both {@link #setBulkActions(int)} and {@link #setBulkSize(ByteSizeValue)}
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
        public BulkProcessor build() {
            return new BulkProcessor(client, listener, concurrentRequests, bulkActions, bulkSize, flushInterval);
        }
    }

    private class Flush implements Runnable {

        @Override
        public void run() {
            synchronized (BulkProcessor.this) {
                if (closed) {
                    return;
                }
                if (bulkRequest.numberOfActions() > 0) {
                    execute();
                }
            }
        }
    }

    interface BulkExecutor {

        void execute(BulkRequest bulkRequest, long executionId);

        boolean awaitClose(long timeout, TimeUnit unit) throws InterruptedException;

    }

    private static class SyncBulkExecutor implements BulkExecutor {

        private final ElasticsearchClient client;

        private final BulkProcessor.Listener listener;

        SyncBulkExecutor(ElasticsearchClient client, BulkProcessor.Listener listener) {
            this.client = client;
            this.listener = listener;
        }

        @Override
        public void execute(BulkRequest bulkRequest, long executionId) {
            boolean afterCalled = false;
            try {
                listener.beforeBulk(executionId, bulkRequest);
                BulkResponse bulkResponse = client.execute(BulkAction.INSTANCE, bulkRequest).actionGet();
                afterCalled = true;
                listener.afterBulk(executionId, bulkRequest, bulkResponse);
            } catch (Exception e) {
                if (!afterCalled) {
                    listener.afterBulk(executionId, bulkRequest, e);
                }
            }
        }

        @Override
        public boolean awaitClose(long timeout, TimeUnit unit) throws InterruptedException {
            return true;
        }
    }

    private static class AsyncBulkExecutor implements BulkExecutor {

        private final ElasticsearchClient client;

        private final BulkProcessor.Listener listener;

        private final Semaphore semaphore;

        private final int concurrentRequests;

        private AsyncBulkExecutor(ElasticsearchClient client, BulkProcessor.Listener listener, int concurrentRequests) {
            this.client = client;
            this.listener = listener;
            this.concurrentRequests = concurrentRequests;
            this.semaphore = new Semaphore(concurrentRequests);
        }

        @Override
        public void execute(final BulkRequest bulkRequest, final long executionId) {
            boolean bulkRequestSetupSuccessful = false;
            boolean acquired = false;
            try {
                listener.beforeBulk(executionId, bulkRequest);
                semaphore.acquire();
                acquired = true;
                client.execute(BulkAction.INSTANCE, bulkRequest, new ActionListener<BulkResponse>() {
                    @Override
                    public void onResponse(BulkResponse response) {
                        try {
                            listener.afterBulk(executionId, bulkRequest, response);
                        } finally {
                            semaphore.release();
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        try {
                            listener.afterBulk(executionId, bulkRequest, e);
                        } finally {
                            semaphore.release();
                        }
                    }
                });
                bulkRequestSetupSuccessful = true;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                listener.afterBulk(executionId, bulkRequest, e);
            } catch (Exception e) {
                listener.afterBulk(executionId, bulkRequest, e);
            } finally {
                if (!bulkRequestSetupSuccessful && acquired) {  // if we fail on client.bulk() release the semaphore
                    semaphore.release();
                }
            }
        }

        @Override
        public boolean awaitClose(long timeout, TimeUnit unit) throws InterruptedException {
            if (semaphore.tryAcquire(this.concurrentRequests, timeout, unit)) {
                semaphore.release(this.concurrentRequests);
                return true;
            }
            return false;
        }
    }
}
