package org.xbib.elx.common;

import org.elasticsearch.common.settings.Settings;
import org.xbib.elx.api.SearchMetric;
import org.xbib.metrics.api.Count;
import org.xbib.metrics.api.Metered;
import org.xbib.metrics.common.CountMetric;
import org.xbib.metrics.common.Meter;
import java.util.concurrent.Executors;

public class DefaultSearchMetric implements SearchMetric {

    private final Meter totalQuery;

    private final Count currentQuery;

    private final Count queries;

    private final Count succeededQueries;

    private final Count emptyQueries;

    private final Count failedQueries;

    private final Count timeoutQueries;

    private Long started;

    private Long stopped;

    public DefaultSearchMetric() {
        totalQuery = new Meter(Executors.newSingleThreadScheduledExecutor());
        currentQuery = new CountMetric();
        queries = new CountMetric();
        succeededQueries = new CountMetric();
        emptyQueries = new CountMetric();
        failedQueries = new CountMetric();
        timeoutQueries = new CountMetric();
    }

    @Override
    public void init(Settings settings) {
        start();
    }

    @Override
    public void markTotalQueries(long n) {
        totalQuery.mark(n);
    }

    @Override
    public Metered getTotalQueries() {
        return totalQuery;
    }

    @Override
    public Count getCurrentQueries() {
        return currentQuery;
    }

    @Override
    public Count getQueries() {
        return queries;
    }

    @Override
    public Count getSucceededQueries() {
        return succeededQueries;
    }

    @Override
    public Count getEmptyQueries() {
        return emptyQueries;
    }

    @Override
    public Count getFailedQueries() {
        return failedQueries;
    }

    @Override
    public Count getTimeoutQueries() {
        return timeoutQueries;
    }

    @Override
    public long elapsed() {
        return started != null ? ((stopped != null ? stopped : System.nanoTime()) - started) : -1L;
    }

    @Override
    public void start() {
        this.started = System.nanoTime();
        totalQuery.start(5L);
    }

    @Override
    public void stop() {
        this.stopped = System.nanoTime();
        totalQuery.stop();
    }

    @Override
    public void close() {
        stop();
        totalQuery.shutdown();
    }
}
