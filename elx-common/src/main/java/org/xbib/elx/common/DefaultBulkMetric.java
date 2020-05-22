package org.xbib.elx.common;

import org.xbib.elx.api.BulkMetric;
import org.xbib.metrics.api.Count;
import org.xbib.metrics.api.Metered;
import org.xbib.metrics.common.CountMetric;
import org.xbib.metrics.common.Meter;

import java.util.concurrent.Executors;

public class DefaultBulkMetric implements BulkMetric {

    private final Meter totalIngest;

    private final Count totalIngestSizeInBytes;

    private final Count currentIngest;

    private final Count currentIngestNumDocs;

    private final Count submitted;

    private final Count succeeded;

    private final Count failed;

    private Long started;

    private Long stopped;

    public DefaultBulkMetric() {
        totalIngest = new Meter(Executors.newSingleThreadScheduledExecutor());
        totalIngestSizeInBytes = new CountMetric();
        currentIngest = new CountMetric();
        currentIngestNumDocs = new CountMetric();
        submitted = new CountMetric();
        succeeded = new CountMetric();
        failed = new CountMetric();
        start();
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
}
