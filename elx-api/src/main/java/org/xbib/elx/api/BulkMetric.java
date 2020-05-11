package org.xbib.elx.api;

import org.xbib.metrics.api.Count;
import org.xbib.metrics.api.Metered;

import java.io.Closeable;

public interface BulkMetric extends Closeable {

    Metered getTotalIngest();

    Count getTotalIngestSizeInBytes();

    Count getCurrentIngest();

    Count getCurrentIngestNumDocs();

    Count getSubmitted();

    Count getSucceeded();

    Count getFailed();

    long elapsed();

    void start();

    void stop();
}
