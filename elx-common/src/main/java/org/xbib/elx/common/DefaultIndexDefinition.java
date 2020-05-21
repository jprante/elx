package org.xbib.elx.common;

import org.xbib.elx.api.IndexDefinition;
import org.xbib.elx.api.IndexRetention;

import java.util.concurrent.TimeUnit;

public class DefaultIndexDefinition implements IndexDefinition {

    private String index;

    private String fullIndexName;

    private String dateTimePattern;

    private String settings;

    private String mappings;

    private boolean enabled;

    private boolean ignoreErrors;

    private boolean switchAliases;

    private boolean hasForceMerge;

    private int replicaLevel;

    private IndexRetention indexRetention;

    private long maxWaitTime;

    private TimeUnit maxWaitTimeUnit;

    private long startRefreshInterval;

    private long stopRefreshInterval;

    @Override
    public IndexDefinition setIndex(String index) {
        this.index = index;
        return this;
    }

    @Override
    public String getIndex() {
        return index;
    }

    @Override
    public IndexDefinition setFullIndexName(String fullIndexName) {
        this.fullIndexName = fullIndexName;
        return this;
    }

    @Override
    public String getFullIndexName() {
        return fullIndexName;
    }

    @Override
    public IndexDefinition setSettings(String settings) {
        this.settings = settings;
        return this;
    }

    @Override
    public String getSettings() {
        return settings;
    }

    @Override
    public IndexDefinition setMappings(String mappings) {
        this.mappings = mappings;
        return this;
    }

    @Override
    public String getMappings() {
        return mappings;
    }

    @Override
    public IndexDefinition setDateTimePattern(String timeWindow) {
        this.dateTimePattern = timeWindow;
        return this;
    }

    @Override
    public String getDateTimePattern() {
        return dateTimePattern;
    }

    @Override
    public IndexDefinition setEnabled(boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    @Override
    public boolean isEnabled() {
        return enabled;
    }

    @Override
    public IndexDefinition setIgnoreErrors(boolean ignoreErrors) {
        this.ignoreErrors = ignoreErrors;
        return this;
    }

    @Override
    public boolean ignoreErrors() {
        return ignoreErrors;
    }

    @Override
    public IndexDefinition setShift(boolean switchAliases) {
        this.switchAliases = switchAliases;
        return this;
    }

    @Override
    public boolean isShiftEnabled() {
        return switchAliases;
    }

    @Override
    public IndexDefinition setForceMerge(boolean hasForceMerge) {
        this.hasForceMerge = hasForceMerge;
        return this;
    }

    @Override
    public boolean hasForceMerge() {
        return hasForceMerge;
    }

    @Override
    public IndexDefinition setReplicaLevel(int replicaLevel) {
        this.replicaLevel = replicaLevel;
        return this;
    }

    @Override
    public int getReplicaLevel() {
        return replicaLevel;
    }

    @Override
    public IndexDefinition setRetention(IndexRetention indexRetention) {
        this.indexRetention = indexRetention;
        return this;
    }

    @Override
    public IndexRetention getRetention() {
        return indexRetention;
    }

    @Override
    public IndexDefinition setMaxWaitTime(long maxWaitTime, TimeUnit timeUnit) {
        this.maxWaitTime = maxWaitTime;
        this.maxWaitTimeUnit = timeUnit;
        return this;
    }

    @Override
    public long getMaxWaitTime() {
        return maxWaitTime;
    }

    @Override
    public TimeUnit getMaxWaitTimeUnit() {
        return maxWaitTimeUnit;
    }

    @Override
    public IndexDefinition setStartRefreshInterval(long seconds) {
        this.startRefreshInterval = seconds;
        return this;
    }

    @Override
    public long getStartRefreshInterval() {
        return startRefreshInterval;
    }

    @Override
    public IndexDefinition setStopRefreshInterval(long seconds) {
        this.stopRefreshInterval = seconds;
        return this;
    }

    @Override
    public long getStopRefreshInterval() {
        return stopRefreshInterval;
    }

}
