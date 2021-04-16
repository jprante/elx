package org.xbib.elx.common;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.xbib.elx.api.BulkController;
import org.xbib.elx.api.BulkListener;
import org.xbib.elx.api.BulkMetric;

import java.io.IOException;
import java.util.Arrays;
import java.util.LongSummaryStatistics;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.LongStream;

public class DefaultBulkListener implements BulkListener {

    private final Logger logger = LogManager.getLogger(DefaultBulkListener.class.getName());

    private final BulkController bulkController;

    private final BulkMetric bulkMetric;

    private final boolean isBulkLoggingEnabled;

    private final boolean failOnError;

    private Throwable lastBulkError;

    private final int ringBufferSize;

    private final LongRingBuffer ringBuffer;

    public DefaultBulkListener(BulkController bulkController,
                               boolean isBulkLoggingEnabled,
                               boolean failOnError,
                               int ringBufferSize) {
        this.bulkController = bulkController;
        this.isBulkLoggingEnabled = isBulkLoggingEnabled;
        this.failOnError = failOnError;
        this.ringBufferSize = ringBufferSize;
        this.ringBuffer = new LongRingBuffer(ringBufferSize);
        this.bulkMetric = new DefaultBulkMetric();
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
        if (ringBufferSize > 0 && ringBuffer.add(response.getTook().millis(), request.estimatedSizeInBytes()) == 0) {
            LongSummaryStatistics stat1 = ringBuffer.longStreamValues1().summaryStatistics();
            LongSummaryStatistics stat2 = ringBuffer.longStreamValues2().summaryStatistics();
            if (isBulkLoggingEnabled && logger.isDebugEnabled()) {
                logger.debug("bulk response millis: avg = " + stat1.getAverage() +
                        " min = " + stat1.getMin() +
                        " max = " + stat1.getMax() +
                        " size: avg = " + stat2.getAverage() +
                        " min = " + stat2.getMin() +
                        " max = " + stat2.getMax() +
                        " throughput: " + (stat2.getAverage() / stat1.getAverage()) + " bytes/ms");
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

    @Override
    public void close() throws IOException {
        bulkMetric.close();
    }

    private static class LongRingBuffer {

       private final Long[] values1, values2;

       private final int limit;

       private final AtomicInteger index;

       public LongRingBuffer(int limit) {
           this.values1 = new Long[limit];
           this.values2 = new Long[limit];
           Arrays.fill(values1, -1L);
           Arrays.fill(values2, -1L);
           this.limit = limit;
           this.index = new AtomicInteger();
       }

       public int add(Long v1, Long v2) {
           int i = index.incrementAndGet() % limit;
           values1[i] = v1;
           values2[i] = v2;
           return i;
       }

       public LongStream longStreamValues1() {
           return Arrays.stream(values1).filter(v -> v != -1L).mapToLong(Long::longValue);
       }

       public LongStream longStreamValues2() {
          return Arrays.stream(values2).filter(v -> v != -1L).mapToLong(Long::longValue);
       }
   }
}
