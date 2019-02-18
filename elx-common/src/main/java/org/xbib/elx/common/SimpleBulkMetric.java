package org.xbib.elx.common;

import org.xbib.elx.api.BulkMetric;
import org.xbib.metrics.Count;
import org.xbib.metrics.CountMetric;
import org.xbib.metrics.Meter;
import org.xbib.metrics.Metered;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class SimpleBulkMetric implements BulkMetric {

    private final Meter totalIngest;

    private final Count totalIngestSizeInBytes;

    private final Count currentIngest;

    private final Count currentIngestNumDocs;

    private final Count submitted;

    private final Count succeeded;

    private final Count failed;

    private Long started;

    private Long stopped;

    public SimpleBulkMetric() {
        this(Executors.newSingleThreadScheduledExecutor());
    }

    public SimpleBulkMetric(ScheduledExecutorService executorService) {
        totalIngest = new Meter(executorService);
        totalIngestSizeInBytes = new CountMetric();
        currentIngest = new CountMetric();
        currentIngestNumDocs = new CountMetric();
        submitted = new CountMetric();
        succeeded = new CountMetric();
        failed = new CountMetric();
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
    public long elapsed() {
        return (stopped != null ? stopped : System.nanoTime()) - started;
    }

}
