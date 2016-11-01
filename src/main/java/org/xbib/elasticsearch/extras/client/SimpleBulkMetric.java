package org.xbib.elasticsearch.extras.client;

import org.xbib.metrics.Count;
import org.xbib.metrics.CountMetric;
import org.xbib.metrics.Meter;
import org.xbib.metrics.Metered;
/**
 *
 */
public class SimpleBulkMetric implements BulkMetric {

    private final Meter totalIngest = new Meter();

    private final Count totalIngestSizeInBytes = new CountMetric();

    private final Count currentIngest = new CountMetric();

    private final Count currentIngestNumDocs = new CountMetric();

    private final Count submitted = new CountMetric();

    private final Count succeeded = new CountMetric();

    private final Count failed = new CountMetric();

    private Long started;

    private Long stopped;

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
        this.totalIngest.spawn(5L);
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
