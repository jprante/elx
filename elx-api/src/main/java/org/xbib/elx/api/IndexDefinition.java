package org.xbib.elx.api;

import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

public interface IndexDefinition {

    IndexDefinition setIndex(String index);

    String getIndex();

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

    IndexDefinition setStartRefreshInterval(long seconds);

    long getStartRefreshInterval();

    IndexDefinition setStopRefreshInterval(long seconds);

    long getStopRefreshInterval();
}
