package org.xbib.elx.api;

import org.elasticsearch.action.ActionRequest;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public interface BulkProcessor extends Closeable, Flushable {

    void setEnabled(boolean enabled);

    void startBulkMode(IndexDefinition indexDefinition) throws IOException;

    void stopBulkMode(IndexDefinition indexDefinition) throws IOException;

    void add(ActionRequest<?> request);

    boolean waitForBulkResponses(long timeout, TimeUnit unit);

    BulkMetric getBulkMetric();

    Throwable getLastBulkError();

    void setMaxBulkActions(int bulkActions);

    int getMaxBulkActions();

    void setMaxBulkVolume(long bulkSize);

    long getMaxBulkVolume();
}
