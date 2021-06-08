package org.xbib.elx.api;

import org.elasticsearch.action.ActionRequest;

import java.io.Closeable;
import java.io.Flushable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public interface BulkProcessor extends Closeable, Flushable {

    void setEnabled(boolean enabled);

    void add(ActionRequest<?> request);

    boolean waitForBulkResponses(long timeout, TimeUnit unit);

    ScheduledExecutorService getScheduler();

    boolean isBulkMetricEnabled();

    BulkMetric getBulkMetric();

    Throwable getLastBulkError();

    void setMaxBulkActions(int bulkActions);

    int getMaxBulkActions();

    void setMaxBulkVolume(long bulkSize);

    long getMaxBulkVolume();

    boolean isClosed();
}
