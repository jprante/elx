package org.xbib.elx.common;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.common.settings.Settings;
import org.xbib.elx.api.BulkListener;
import org.xbib.elx.api.BulkMetric;
import org.xbib.elx.api.BulkProcessor;

import java.io.IOException;

public class DefaultBulkListener implements BulkListener {

    private final Logger logger = LogManager.getLogger(DefaultBulkListener.class.getName());

    private final BulkProcessor bulkProcessor;

    private final boolean failOnError;

    private BulkMetric bulkMetric;

    private Throwable lastBulkError;

    public DefaultBulkListener(DefaultBulkProcessor bulkProcessor,
                               Settings settings) {
        this.bulkProcessor = bulkProcessor;
        this.failOnError = settings.getAsBoolean(Parameters.BULK_FAIL_ON_ERROR.getName(),
                Parameters.BULK_FAIL_ON_ERROR.getBoolean());
        if (settings.getAsBoolean(Parameters.BULK_METRIC_ENABLED.getName(),
                Parameters.BULK_METRIC_ENABLED.getBoolean())) {
            this.bulkMetric = new DefaultBulkMetric(bulkProcessor, bulkProcessor.getScheduler(), settings);
            bulkMetric.start();
        }
    }

    public BulkMetric getBulkMetric() {
        return bulkMetric;
    }

    @Override
    public void beforeBulk(long executionId, BulkRequest request) {
        if (bulkMetric != null) {
            long l = bulkMetric.getCurrentIngest().getCount();
            bulkMetric.getCurrentIngest().inc();
            int n = request.numberOfActions();
            bulkMetric.getSubmitted().inc(n);
            bulkMetric.getCurrentIngestNumDocs().inc(n);
            bulkMetric.getTotalIngestSizeInBytes().inc(request.estimatedSizeInBytes());
            if (logger.isDebugEnabled()) {
                logger.debug("before bulk [{}] [actions={}] [bytes={}] [requests={}]",
                        executionId,
                        request.numberOfActions(),
                        request.estimatedSizeInBytes(),
                        l);
            }
        }
    }

    @Override
    public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
        long l = 0L;
        if (bulkMetric != null) {
            bulkMetric.recalculate(request, response);
            l = bulkMetric.getCurrentIngest().getCount();
            bulkMetric.getCurrentIngest().dec();
            bulkMetric.getSucceeded().inc(response.getItems().length);
            bulkMetric.markTotalIngest(response.getItems().length);
        }
        int n = 0;
        for (BulkItemResponse itemResponse : response.getItems()) {
            if (bulkMetric != null) {
                bulkMetric.getCurrentIngest().dec(itemResponse.getIndex(), itemResponse.getType(), itemResponse.getId());
            }
            if (itemResponse.isFailed()) {
                n++;
                if (bulkMetric != null) {
                    bulkMetric.getSucceeded().dec(1);
                    bulkMetric.getFailed().inc(1);
                }
            }
        }
        if (bulkMetric != null) {
            if (logger.isDebugEnabled()) {
                logger.debug("after bulk [{}] [succeeded={}] [failed={}] [{}ms] [requests={}]",
                        executionId,
                        bulkMetric.getSucceeded().getCount(),
                        bulkMetric.getFailed().getCount(),
                        response.getTook().millis(),
                        l);
            }
        }
        if (n > 0) {
            if (logger.isErrorEnabled()) {
                logger.error("bulk [{}] failed with {} failed items, failure message = {}",
                        executionId, n, response.buildFailureMessage());
            }
            if (failOnError) {
                throw new IllegalStateException("bulk failed: id = " + executionId +
                        " n = " + n + " message = " + response.buildFailureMessage());
            }
        } else {
            if (bulkMetric != null) {
                bulkMetric.getCurrentIngestNumDocs().dec(response.getItems().length);
            }
        }
    }

    @Override
    public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
        if (bulkMetric != null) {
            bulkMetric.getCurrentIngest().dec();
        }
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
        if (bulkMetric != null) {
            bulkMetric.close();
        }
    }
}
