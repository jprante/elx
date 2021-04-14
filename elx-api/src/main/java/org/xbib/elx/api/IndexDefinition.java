package org.xbib.elx.api;

import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

public interface IndexDefinition {

    /**
     * The one and only index type name used in the extended client.
     * Note that all Elasticsearch version &lt; 6.2.0 do not allow a prepending "_".
     */
    String TYPE_NAME = "_doc";

    IndexDefinition setIndex(String index);

    String getIndex();

    IndexDefinition setType(String type);

    String getType();

    IndexDefinition setFullIndexName(String fullIndexName);

    String getFullIndexName();

    IndexDefinition setSettings(String settings);

    String getSettings();

    IndexDefinition setMappings(String mappings);

    String getMappings();

    IndexDefinition setDateTimeFormatter(DateTimeFormatter formatter);

    DateTimeFormatter getDateTimeFormatter();

    IndexDefinition setDateTimePattern(Pattern pattern);

    Pattern getDateTimePattern();

    IndexDefinition setStartBulkRefreshSeconds(int seconds);

    int getStartBulkRefreshSeconds();

    IndexDefinition setStopBulkRefreshSeconds(int seconds);

    int getStopBulkRefreshSeconds();

    IndexDefinition setEnabled(boolean enabled);

    boolean isEnabled();

    IndexDefinition setIgnoreErrors(boolean ignoreErrors);

    boolean ignoreErrors();

    IndexDefinition setShift(boolean shift);

    boolean isShiftEnabled();

    IndexDefinition setPrune(boolean prune);

    boolean isPruneEnabled();

    IndexDefinition setForceMerge(boolean forcemerge);

    boolean isForceMergeEnabled();

    IndexDefinition setReplicaLevel(int replicaLevel);

    int getReplicaLevel();

    IndexDefinition setRetention(IndexRetention indexRetention);

    IndexRetention getRetention();

    IndexDefinition setMaxWaitTime(long maxWaitTime, TimeUnit timeUnit);

    long getMaxWaitTime();

    TimeUnit getMaxWaitTimeUnit();
}
