package org.xbib.elx.api;

import org.elasticsearch.action.DocWriteRequest;

import java.io.Closeable;
import java.io.Flushable;
import java.util.concurrent.TimeUnit;

public interface BulkProcessor extends Closeable, Flushable {

    void setBulkActions(int bulkActions);

    int getBulkActions();

    void setBulkSize(long bulkSize);

    long getBulkSize();

    BulkProcessor add(DocWriteRequest<?> request);

    boolean awaitFlush(long timeout, TimeUnit unit) throws InterruptedException;

    boolean awaitClose(long timeout, TimeUnit unit) throws InterruptedException;
}
