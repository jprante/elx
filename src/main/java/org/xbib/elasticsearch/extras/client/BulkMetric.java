package org.xbib.elasticsearch.extras.client;

import org.xbib.metrics.Count;
import org.xbib.metrics.Metered;

/**
 *
 */
public interface BulkMetric {

    Metered getTotalIngest();

    Count getTotalIngestSizeInBytes();

    Count getCurrentIngest();

    Count getCurrentIngestNumDocs();

    Count getSubmitted();

    Count getSucceeded();

    Count getFailed();

    void start();

    void stop();

    long elapsed();

}
