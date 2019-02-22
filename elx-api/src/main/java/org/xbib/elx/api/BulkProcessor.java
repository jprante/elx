package org.xbib.elx.api;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;

import java.io.Closeable;
import java.io.Flushable;
import java.util.concurrent.TimeUnit;

public interface BulkProcessor extends Closeable, Flushable {

    @SuppressWarnings("rawtype")
    BulkProcessor add(ActionRequest request);

    @SuppressWarnings("rawtype")
    BulkProcessor add(ActionRequest request, Object payload);

    boolean awaitFlush(long timeout, TimeUnit unit) throws InterruptedException;

    boolean awaitClose(long timeout, TimeUnit unit) throws InterruptedException;

    interface BulkRequestHandler {

        void execute(BulkRequest bulkRequest, long executionId);

        boolean close(long timeout, TimeUnit unit) throws InterruptedException;

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
}
