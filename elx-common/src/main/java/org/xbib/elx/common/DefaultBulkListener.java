package org.xbib.elx.common;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.xbib.elx.api.BulkController;
import org.xbib.elx.api.BulkListener;
import org.xbib.elx.api.BulkMetric;

public class DefaultBulkListener implements BulkListener {

    private final Logger logger = LogManager.getLogger(DefaultBulkListener.class.getName());

    private final BulkController bulkController;

    private final BulkMetric bulkMetric;

    private final boolean isBulkLoggingEnabled;

    private Throwable lastBulkError;

    public DefaultBulkListener(BulkController bulkController,
                               BulkMetric bulkMetric,
                               boolean isBulkLoggingEnabled) {
        this.bulkController = bulkController;
        this.bulkMetric = bulkMetric;
        this.isBulkLoggingEnabled = isBulkLoggingEnabled;
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
            logger.debug("before bulk [{}] [actions={}] [bytes={}] [concurrent requests={}]",
                    executionId,
                    request.numberOfActions(),
                    request.estimatedSizeInBytes(),
                    l);
        }
    }

    @Override
    public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
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
            logger.debug("after bulk [{}] [succeeded={}] [failed={}] [{}ms] {} concurrent requests",
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
        bulkController.inactivate();
    }

    @Override
    public Throwable getLastBulkError() {
        return lastBulkError;
    }
}
