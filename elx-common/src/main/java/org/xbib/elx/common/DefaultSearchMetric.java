package org.xbib.elx.common;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.xbib.elx.api.SearchClient;
import org.xbib.elx.api.SearchMetric;
import org.xbib.metrics.api.Count;
import org.xbib.metrics.api.Metered;
import org.xbib.metrics.common.CountMetric;
import org.xbib.metrics.common.Meter;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class DefaultSearchMetric implements SearchMetric {

    private static final Logger logger = LogManager.getLogger(DefaultSearchMetric.class.getName());

    private final ScheduledFuture<?> future;

    private final Meter totalQuery;

    private final Count currentQuery;

    private final Count queries;

    private final Count succeededQueries;

    private final Count emptyQueries;

    private final Count failedQueries;

    private final Count timeoutQueries;

    private Long started;

    private Long stopped;

    private final AtomicBoolean closed;

    public DefaultSearchMetric(SearchClient searchClient,
                               Settings settings) {
        this.closed = new AtomicBoolean(true);
        totalQuery = new Meter(searchClient.getScheduler());
        currentQuery = new CountMetric();
        queries = new CountMetric();
        succeededQueries = new CountMetric();
        emptyQueries = new CountMetric();
        failedQueries = new CountMetric();
        timeoutQueries = new CountMetric();
        String metricLogIntervalStr = settings.get(Parameters.SEARCH_METRIC_LOG_INTERVAL.getName(),
                Parameters.SEARCH_METRIC_LOG_INTERVAL.getString());
        TimeValue metricLoginterval = TimeValue.parseTimeValue(metricLogIntervalStr,
                TimeValue.timeValueSeconds(10), "");
        this.future = searchClient.getScheduler().scheduleAtFixedRate(this::log, 0L, metricLoginterval.seconds(), TimeUnit.SECONDS);
    }

    @Override
    public void init(Settings settings) {
        if (closed.compareAndSet(true, false)) {
            start();
        }
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
        log();
        this.future.cancel(true);
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            stop();
            totalQuery.shutdown();
        }
    }

    private void log() {
        if (logger.isInfoEnabled()) {
            logger.info("queries = " + getTotalQueries().getCount() +
                    " succeeded = " + getSucceededQueries().getCount() +
                    " empty = " + getEmptyQueries().getCount() +
                    " failed = " + getFailedQueries().getCount() +
                    " timeouts = " + getTimeoutQueries().getCount());
        }
    }
}
