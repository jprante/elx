package org.xbib.elx.api;

import org.elasticsearch.action.bulk.BulkRequest;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

public interface BulkRequestHandler {

    void execute(BulkRequest bulkRequest);

    boolean flush(long timeout, TimeUnit unit) throws IOException, InterruptedException;

    int getPermits();

    void increase();

    void reduce();
}
