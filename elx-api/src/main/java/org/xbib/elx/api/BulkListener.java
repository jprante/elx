package org.xbib.elx.api;

import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;

public interface BulkListener {

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

    /**
     * Get the last bulk error.
     * @return the last bulk error
     */
    Throwable getLastBulkError();
}
