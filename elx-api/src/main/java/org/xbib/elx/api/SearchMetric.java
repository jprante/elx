package org.xbib.elx.api;

import org.elasticsearch.common.settings.Settings;
import org.xbib.metrics.api.Count;
import org.xbib.metrics.api.Metered;
import java.io.Closeable;

public interface SearchMetric extends Closeable {

    void init(Settings settings);

    void markTotalQueries(long n);

    Metered getTotalQueries();

    Count getCurrentQueries();

    Count getQueries();

    Count getSucceededQueries();

    Count getEmptyQueries();

    long elapsed();

    void start();

    void stop();
}
