package org.xbib.elx.common;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.xbib.elx.api.BulkController;
import org.xbib.elx.api.BulkListener;
import org.xbib.elx.api.BulkMetric;
import java.util.Arrays;
import java.util.LongSummaryStatistics;
import java.util.stream.LongStream;

public class DefaultBulkListener implements BulkListener {

    private final Logger logger = LogManager.getLogger(DefaultBulkListener.class.getName());

    private final BulkController bulkController;

    private final BulkMetric bulkMetric;

    private final boolean isBulkLoggingEnabled;

    private final boolean failOnError;

    private Throwable lastBulkError;

    private final int responseTimeCount;

    private final LastResponseTimes responseTimes;

    public DefaultBulkListener(BulkController bulkController,
                               BulkMetric bulkMetric,
                               boolean isBulkLoggingEnabled,
                               boolean failOnError,
                               int responseTimeCount) {
        this.bulkController = bulkController;
        this.bulkMetric = bulkMetric;
        this.isBulkLoggingEnabled = isBulkLoggingEnabled;
        this.failOnError = failOnError;
        this.responseTimeCount = responseTimeCount;
        this.responseTimes = new LastResponseTimes(responseTimeCount);
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
        bulkMetric.markTotalIngest(response.getItems().length);
        if (responseTimeCount > 0 && responseTimes.add(response.getTook().millis()) == 0) {
            LongSummaryStatistics stat = responseTimes.longStream().summaryStatistics();
            if (isBulkLoggingEnabled && logger.isDebugEnabled()) {
                logger.debug("bulk response millis: avg = " + stat.getAverage() +
                        " min =" + stat.getMin() +
                        " max = " + stat.getMax() +
                        " actions = " + bulkController.getBulkProcessor().getBulkActions() +
                        " size = " + bulkController.getBulkProcessor().getBulkSize());
            }
        }
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
            logger.debug("after bulk [{}] [succeeded={}] [failed={}] [{}ms] [concurrent requests={}]",
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
        bulkController.inactivate();
    }

    @Override
    public Throwable getLastBulkError() {
        return lastBulkError;
    }

    private static class LastResponseTimes {

        private final Long[] values;

        private final int limit;

        private int index;

        public LastResponseTimes(int limit) {
            this.values = new Long[limit];
            Arrays.fill(values, -1L);
            this.limit = limit;
            this.index = 0;
        }

        public int add(Long value) {
            int i = index++ % limit;
            values[i] = value;
            return i;
        }

        public LongStream longStream() {
            return Arrays.stream(values).filter(v -> v != -1L).mapToLong(Long::longValue);
        }
    }
}
