package org.xbib.elx.api;

import org.elasticsearch.action.DocWriteRequest;

import java.io.Closeable;
import java.io.Flushable;
import java.util.concurrent.TimeUnit;

public interface BulkProcessor extends Closeable, Flushable {

    void setEnabled(boolean enabled);

    void add(DocWriteRequest<?> request);

    boolean waitForBulkResponses(long timeout, TimeUnit unit);

    BulkMetric getBulkMetric();

    Throwable getLastBulkError();

    void setMaxBulkActions(int bulkActions);

    int getMaxBulkActions();

    void setMaxBulkVolume(long bulkSize);

    long getMaxBulkVolume();
}
