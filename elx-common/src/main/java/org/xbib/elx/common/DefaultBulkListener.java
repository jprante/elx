package org.xbib.elx.common;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.xbib.elx.api.BulkListener;
import org.xbib.elx.api.BulkMetric;
import org.xbib.elx.api.BulkProcessor;

import java.io.IOException;
import java.util.LongSummaryStatistics;
import java.util.concurrent.ScheduledThreadPoolExecutor;

public class DefaultBulkListener implements BulkListener {

    private final Logger logger = LogManager.getLogger(DefaultBulkListener.class.getName());

    private final BulkProcessor bulkProcessor;

    private final BulkMetric bulkMetric;

    private final boolean isBulkLoggingEnabled;

    private final boolean failOnError;

    private Throwable lastBulkError;

    public DefaultBulkListener(BulkProcessor bulkProcessor,
                               ScheduledThreadPoolExecutor scheduler,
                               boolean isBulkLoggingEnabled,
                               boolean failOnError,
                               int ringBufferSize) {
        this.bulkProcessor = bulkProcessor;
        this.isBulkLoggingEnabled = isBulkLoggingEnabled;
        this.failOnError = failOnError;
        this.bulkMetric = new DefaultBulkMetric(bulkProcessor, scheduler, ringBufferSize);
        bulkMetric.start();
    }

    public BulkMetric getBulkMetric() {
        return bulkMetric;
    }

    @Override
    public void beforeBulk(long executionId, BulkRequest request) {
        long l = bulkMetric.getCurrentIngest().getCount();
        bulkMetric.getCurrentIngest().inc();
        int n = request.numberOfActions();
        bulkMetric.getSubmitted().inc(n);
        bulkMetric.getCurrentIngestNumDocs().inc(n);
        bulkMetric.getTotalIngestSizeInBytes().inc(request.estimatedSizeInBytes());
        if (isBulkLoggingEnabled && logger.isDebugEnabled()) {
            logger.debug("before bulk [{}] [actions={}] [bytes={}] [requests={}]",
                    executionId,
                    request.numberOfActions(),
                    request.estimatedSizeInBytes(),
                    l);
        }
    }

    @Override
    public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
        bulkMetric.recalculate(request, response);
        long l = bulkMetric.getCurrentIngest().getCount();
        bulkMetric.getCurrentIngest().dec();
        bulkMetric.getSucceeded().inc(response.getItems().length);
        int n = 0;
        for (BulkItemResponse itemResponse : response.getItems()) {
            bulkMetric.getCurrentIngest().dec(itemResponse.getIndex(), itemResponse.getType(), itemResponse.getId());
            if (itemResponse.isFailed()) {
                n++;
                bulkMetric.getSucceeded().dec(1);
                bulkMetric.getFailed().inc(1);
            }
        }
        if (isBulkLoggingEnabled && logger.isDebugEnabled()) {
            logger.debug("after bulk [{}] [succeeded={}] [failed={}] [{}ms] [requests={}]",
                    executionId,
                    bulkMetric.getSucceeded().getCount(),
                    bulkMetric.getFailed().getCount(),
                    response.getTook().millis(),
                    l);
        }
        if (n > 0) {
            if (isBulkLoggingEnabled && logger.isErrorEnabled()) {
                logger.error("bulk [{}] failed with {} failed items, failure message = {}",
                        executionId, n, response.buildFailureMessage());
            }
            if (failOnError) {
                throw new IllegalStateException("bulk failed: id = " + executionId +
                        " n = " + n + " message = " + response.buildFailureMessage());
            }
        } else {
            bulkMetric.getCurrentIngestNumDocs().dec(response.getItems().length);
        }
    }

    @Override
    public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
        bulkMetric.getCurrentIngest().dec();
        lastBulkError = failure;
        if (logger.isErrorEnabled()) {
            logger.error("after bulk [" + executionId + "] error", failure);
        }
        bulkProcessor.setEnabled(false);
    }

    @Override
    public Throwable getLastBulkError() {
        return lastBulkError;
    }

    @Override
    public void close() throws IOException {
        bulkMetric.close();
    }
}
