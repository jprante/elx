package org.xbib.elx.common;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.xbib.elx.api.BulkMetric;
import org.xbib.elx.api.BulkProcessor;
import org.xbib.metrics.api.Count;
import org.xbib.metrics.api.Metered;
import org.xbib.metrics.common.CountMetric;
import org.xbib.metrics.common.Meter;

import java.util.LongSummaryStatistics;
import java.util.concurrent.ScheduledThreadPoolExecutor;

public class DefaultBulkMetric implements BulkMetric {

    private static final Logger logger = LogManager.getLogger(DefaultBulkMetric.class.getName());

    private final BulkProcessor bulkProcessor;

    private final Meter totalIngest;

    private final Count totalIngestSizeInBytes;

    private final Count currentIngest;

    private final Count currentIngestNumDocs;

    private final Count submitted;

    private final Count succeeded;

    private final Count failed;

    private Long started;

    private Long stopped;

    private final int ringBufferSize;

    private final LongRingBuffer ringBuffer;

    private Double lastThroughput;

    private long currentMaxVolume;

    private int currentPermits;

    public DefaultBulkMetric(BulkProcessor bulkProcessor,
                             ScheduledThreadPoolExecutor scheduledThreadPoolExecutor,
                             int ringBufferSize) {
        this.bulkProcessor = bulkProcessor;
        this.totalIngest = new Meter(scheduledThreadPoolExecutor);
        this.ringBufferSize = ringBufferSize;
        this.ringBuffer = new LongRingBuffer(ringBufferSize);
        this.totalIngestSizeInBytes = new CountMetric();
        this.currentIngest = new CountMetric();
        this.currentIngestNumDocs = new CountMetric();
        this.submitted = new CountMetric();
        this.succeeded = new CountMetric();
        this.failed = new CountMetric();
        this.currentMaxVolume = 1024;
        this.currentPermits = 1;
    }

    @Override
    public void markTotalIngest(long n) {
        totalIngest.mark(n);
    }

    @Override
    public Metered getTotalIngest() {
        return totalIngest;
    }

    @Override
    public Count getTotalIngestSizeInBytes() {
        return totalIngestSizeInBytes;
    }

    @Override
    public Count getCurrentIngest() {
        return currentIngest;
    }

    @Override
    public Count getCurrentIngestNumDocs() {
        return currentIngestNumDocs;
    }

    @Override
    public Count getSubmitted() {
        return submitted;
    }

    @Override
    public Count getSucceeded() {
        return succeeded;
    }

    @Override
    public Count getFailed() {
        return failed;
    }

    @Override
    public long elapsed() {
        return started != null ? ((stopped != null ? stopped : System.nanoTime()) - started) : -1L;
    }

    @Override
    public void start() {
        this.started = System.nanoTime();
        totalIngest.start(5L);
    }

    @Override
    public void stop() {
        this.stopped = System.nanoTime();
        totalIngest.stop();
    }

    @Override
    public void close() {
        stop();
        totalIngest.shutdown();
    }

    private int x = 0;

    @Override
    public void recalculate(BulkRequest request, BulkResponse response) {
        if (ringBufferSize > 0 && ringBuffer.add(response.getTook().millis(), request.estimatedSizeInBytes()) == 0) {
            x++;
            LongSummaryStatistics stat1 = ringBuffer.longStreamValues1().summaryStatistics();
            LongSummaryStatistics stat2 = ringBuffer.longStreamValues2().summaryStatistics();
            double throughput = stat2.getAverage() / stat1.getAverage();
            double delta = lastThroughput != null ? throughput - lastThroughput : 0.0d;
            if (logger.isDebugEnabled()) {
                logger.debug("metric: avg = " + stat1.getAverage() +
                        " min = " + stat1.getMin() +
                        " max = " + stat1.getMax() +
                        " size: avg = " + stat2.getAverage() +
                        " min = " + stat2.getMin() +
                        " max = " + stat2.getMax() +
                        " last throughput: " + lastThroughput + " bytes/ms" +
                        " throughput: " + throughput + " bytes/ms" +
                        " delta = " + delta +
                        " vol = " + currentMaxVolume);
            }
            if (lastThroughput == null || throughput < 10000) {
                double k = 0.5;
                double d = (1 / (1 + Math.exp(-(((double)x)) * k)));
                logger.debug("inc: x = " + x + " d = " + d);
                currentMaxVolume += d * currentMaxVolume;
                if (currentMaxVolume > 5 + 1024 * 1024) {
                    currentMaxVolume = 5 * 1024 * 1024;
                }
                bulkProcessor.setMaxBulkVolume(currentMaxVolume);
                if (logger.isDebugEnabled()) {
                    logger.debug("metric: increase volume to " + currentMaxVolume);
                }
            } else if (delta < -100) {
                double k = 0.5;
                double d = (1 / (1 + Math.exp(-(((double)x)) * k)));
                d = -1/d;
                logger.debug("dec: x = " + x + " d = " + d);
                currentMaxVolume += d * currentMaxVolume;
                if (currentMaxVolume > 5 + 1024 * 1024) {
                    currentMaxVolume = 5 * 1024 * 1024;
                }
                bulkProcessor.setMaxBulkVolume(currentMaxVolume);
                if (logger.isDebugEnabled()) {
                    logger.debug("metric: decrease volume to " + currentMaxVolume);
                }
            }
            lastThroughput = throughput;
        }
    }
}
