package org.xbib.elx.api;

import org.elasticsearch.action.bulk.BulkRequest;
import java.util.concurrent.TimeUnit;

public interface BulkRequestHandler {

    void execute(BulkRequest bulkRequest, long executionId);

    boolean close(long timeout, TimeUnit unit) throws InterruptedException;
}
