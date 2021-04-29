package org.xbib.elx.common;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.xbib.elx.api.BulkMetric;
import org.xbib.metrics.api.Count;
import org.xbib.metrics.api.Metered;
import org.xbib.metrics.common.CountMetric;
import org.xbib.metrics.common.Meter;

import java.io.IOException;
import java.util.LongSummaryStatistics;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class DefaultBulkMetric implements BulkMetric {

    private static final Logger logger = LogManager.getLogger(DefaultBulkMetric.class.getName());

    private final DefaultBulkProcessor bulkProcessor;

    private final ScheduledFuture<?> future;

    private final Meter totalIngest;

    private final Count totalIngestSizeInBytes;

    private final Count currentIngest;

    private final Count currentIngestNumDocs;

    private final Count submitted;

    private final Count succeeded;

    private final Count failed;

    private final long measureIntervalSeconds;

    private final int ringBufferSize;

    private final LongRingBuffer ringBuffer;

    private final long minVolumePerRequest;

    private long maxVolumePerRequest;

    private Long started;

    private Long stopped;

    private Double lastThroughput;

    private long currentVolumePerRequest;

    private int x = 0;

    public DefaultBulkMetric(DefaultBulkProcessor bulkProcessor,
                             ScheduledExecutorService scheduledExecutorService,
                             Settings settings) {
        this.bulkProcessor = bulkProcessor;
        int ringBufferSize = settings.getAsInt(Parameters.BULK_RING_BUFFER_SIZE.getName(),
                Parameters.BULK_RING_BUFFER_SIZE.getInteger());
        String measureIntervalStr = settings.get(Parameters.BULK_MEASURE_INTERVAL.getName(),
                Parameters.BULK_MEASURE_INTERVAL.getString());
        TimeValue measureInterval = TimeValue.parseTimeValue(measureIntervalStr,
                TimeValue.timeValueSeconds(1), "");
        this.measureIntervalSeconds = measureInterval.seconds();
        this.totalIngest = new Meter(scheduledExecutorService);
        this.ringBufferSize = ringBufferSize;
        this.ringBuffer = new LongRingBuffer(ringBufferSize);
        this.totalIngestSizeInBytes = new CountMetric();
        this.currentIngest = new CountMetric();
        this.currentIngestNumDocs = new CountMetric();
        this.submitted = new CountMetric();
        this.succeeded = new CountMetric();
        this.failed = new CountMetric();
        ByteSizeValue minVolumePerRequest = settings.getAsBytesSize(Parameters.BULK_MIN_VOLUME_PER_REQUEST.getName(),
                ByteSizeValue.parseBytesSizeValue(Parameters.BULK_MIN_VOLUME_PER_REQUEST.getString(), "1k"));
        this.minVolumePerRequest = minVolumePerRequest.getBytes();
        ByteSizeValue maxVolumePerRequest = settings.getAsBytesSize(Parameters.BULK_MAX_VOLUME_PER_REQUEST.getName(),
                ByteSizeValue.parseBytesSizeValue(Parameters.BULK_MAX_VOLUME_PER_REQUEST.getString(), "1m"));
        this.maxVolumePerRequest = maxVolumePerRequest.getBytes();
        this.currentVolumePerRequest = minVolumePerRequest.getBytes();
        String metricLogIntervalStr = settings.get(Parameters.BULK_METRIC_LOG_INTERVAL.getName(),
                Parameters.BULK_METRIC_LOG_INTERVAL.getString());
        TimeValue metricLoginterval = TimeValue.parseTimeValue(metricLogIntervalStr,
                TimeValue.timeValueSeconds(10), "");
        this.future = scheduledExecutorService.scheduleAtFixedRate(this::log, 0L, metricLoginterval.seconds(), TimeUnit.SECONDS);
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
        totalIngest.start(measureIntervalSeconds);
    }

    @Override
    public void stop() {
        this.stopped = System.nanoTime();
        totalIngest.stop();
    }

    @Override
    public void close() throws IOException {
        stop();
        totalIngest.shutdown();
        log();
        this.future.cancel(true);
    }

    @Override
    public void recalculate(BulkRequest request, BulkResponse response) {
        if (ringBufferSize > 0 && ringBuffer.add(response.getTook().millis(), request.estimatedSizeInBytes()) == 0) {
            x++;
            LongSummaryStatistics stat1 = ringBuffer.longStreamValues1().summaryStatistics();
            LongSummaryStatistics stat2 = ringBuffer.longStreamValues2().summaryStatistics();
            double throughput = stat2.getAverage() / stat1.getAverage();
            double delta = lastThroughput != null ? throughput - lastThroughput : 0.0d;
            double deltaPercent = delta * 100 / throughput;
            if (logger.isDebugEnabled()) {
                logger.debug("time: avg = " + stat1.getAverage() +
                        " min = " + stat1.getMin() +
                        " max = " + stat1.getMax() +
                        " size: avg = " + stat2.getAverage() +
                        " min = " + stat2.getMin() +
                        " max = " + stat2.getMax() +
                        " last throughput: " + lastThroughput + " bytes/ms" +
                        " throughput: " + throughput + " bytes/ms" +
                        " delta = " + delta +
                        " deltapercent = " + deltaPercent +
                        " vol = " + currentVolumePerRequest);
            }
            if ((lastThroughput == null || throughput < 1000000) && stat1.getAverage() < 5000) {
                double k = 0.5;
                double d = (1 / (1 + Math.exp(-(((double)x)) * k)));
                currentVolumePerRequest += d * currentVolumePerRequest;
                if (currentVolumePerRequest > maxVolumePerRequest) {
                    currentVolumePerRequest = maxVolumePerRequest;
                } else {
                    if (logger.isDebugEnabled()) {
                        logger.debug("increasing volume to " + currentVolumePerRequest + " max volume = " + maxVolumePerRequest);
                    }
                }
                bulkProcessor.setMaxBulkVolume(currentVolumePerRequest);
            } else if (stat1.getAverage() >= 5000) {
                if (currentVolumePerRequest == maxVolumePerRequest) {
                    // subtract 10% from max
                    this.maxVolumePerRequest -= (maxVolumePerRequest / 10);
                    if (maxVolumePerRequest < 1024) {
                        maxVolumePerRequest = 1024;
                    }
                }
                // fall back to minimal volume
                currentVolumePerRequest = minVolumePerRequest;
                bulkProcessor.setMaxBulkVolume(currentVolumePerRequest);
                if (logger.isDebugEnabled()) {
                    logger.debug("decreasing volume to " + currentVolumePerRequest + " new max volume = " + maxVolumePerRequest);
                }
            }
            lastThroughput = throughput;
        }
    }

    private void log() {
        long docs = getSucceeded().getCount();
        long elapsed = elapsed() / 1000000; // nano to millis
        double dps = docs * 1000.0 / elapsed;
        long bytes = getTotalIngestSizeInBytes().getCount();
        double avg = bytes / (docs + 1.0); // avoid div by zero
        double bps = bytes * 1000.0 / elapsed;
        if (logger.isInfoEnabled()) {
            logger.log(Level.INFO, "{} docs, {} ms = {}, {} = {}, {} = {} avg, {} = {}, {} = {}",
                    docs,
                    elapsed,
                    FormatUtil.formatDurationWords(elapsed, true, true),
                    bytes,
                    FormatUtil.formatSize(bytes),
                    avg,
                    FormatUtil.formatSize(avg),
                    dps,
                    FormatUtil.formatDocumentSpeed(dps),
                    bps,
                    FormatUtil.formatSpeed(bps)
            );
        }
    }
}
