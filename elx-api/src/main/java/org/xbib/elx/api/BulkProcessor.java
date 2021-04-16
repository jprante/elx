package org.xbib.elx.api;

import org.elasticsearch.action.ActionRequest;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

public interface BulkProcessor extends Closeable, Flushable {

    void setBulkActions(int bulkActions);

    int getBulkActions();

    void setBulkSize(long bulkSize);

    long getBulkSize();

    BulkRequestHandler getBulkRequestHandler();

    void add(ActionRequest<?> request);

    boolean awaitFlush(long timeout, TimeUnit unit) throws InterruptedException, IOException;
}
