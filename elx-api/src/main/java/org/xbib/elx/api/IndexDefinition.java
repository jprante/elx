package org.xbib.elx.api;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.concurrent.TimeUnit;

public interface IndexDefinition {

    IndexDefinition setIndex(String index);

    String getIndex();

    IndexDefinition setFullIndexName(String fullIndexName);

    String getFullIndexName();

    IndexDefinition setSettingsUrl(String settingsUrlString) throws MalformedURLException;

    IndexDefinition setSettingsUrl(URL settingsUrl);

    URL getSettingsUrl();

    IndexDefinition setMappingsUrl(String mappingsUrlString) throws MalformedURLException;

    IndexDefinition setMappingsUrl(URL mappingsUrl);

    URL getMappingsUrl();

    IndexDefinition setDateTimePattern(String timeWindow);

    String getDateTimePattern();

    IndexDefinition setEnabled(boolean enabled);

    boolean isEnabled();

    IndexDefinition setIgnoreErrors(boolean ignoreErrors);

    boolean ignoreErrors();

    IndexDefinition setShift(boolean shift);

    boolean isShiftEnabled();

    IndexDefinition setForceMerge(boolean hasForceMerge);

    boolean hasForceMerge();

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
