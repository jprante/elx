package org.xbib.elx.api;

import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.settings.Settings;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

public interface BulkController extends Closeable, Flushable {

    void init(Settings settings);

    void inactivate();

    BulkMetric getBulkMetric();

    Throwable getLastBulkError();

    void startBulkMode(IndexDefinition indexDefinition) throws IOException;

    void startBulkMode(String indexName, long startRefreshIntervalInSeconds,
                       long stopRefreshIntervalInSeconds) throws IOException;

    void bulkIndex(IndexRequest indexRequest);

    void bulkDelete(DeleteRequest deleteRequest);

    void bulkUpdate(UpdateRequest updateRequest);

    boolean waitForBulkResponses(long timeout, TimeUnit timeUnit);

    void stopBulkMode(IndexDefinition indexDefinition) throws IOException;

    void stopBulkMode(String index, long timeout, TimeUnit timeUnit) throws IOException;
}
