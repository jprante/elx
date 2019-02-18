package org.xbib.elx.common.management;

import java.net.URL;

public class IndexDefinition {

    private String index;

    private String type;

    private String fullIndexName;

    private String dateTimePattern;

    private URL settingsUrl;

    private URL mappingsUrl;

    private boolean enabled;

    private boolean ignoreErrors;

    private boolean switchAliases;

    private boolean hasForceMerge;

    private int replicaLevel;

    private IndexRetention indexRetention;

    public IndexDefinition setIndex(String index) {
        this.index = index;
        return this;
    }

    public String getIndex() {
        return index;
    }

    public IndexDefinition setFullIndexName(String fullIndexName) {
        this.fullIndexName = fullIndexName;
        return this;
    }

    public String getFullIndexName() {
        return fullIndexName;
    }

    public IndexDefinition setType(String type) {
        this.type = type;
        return this;
    }

    public String getType() {
        return type;
    }

    public IndexDefinition setSettingsUrl(URL settingsUrl) {
        this.settingsUrl = settingsUrl;
        return this;
    }

    public URL getSettingsUrl() {
        return settingsUrl;
    }

    public IndexDefinition setMappingsUrl(URL mappingsUrl) {
        this.mappingsUrl = mappingsUrl;
        return this;
    }

    public URL getMappingsUrl() {
        return mappingsUrl;
    }

    public IndexDefinition setDateTimePattern(String timeWindow) {
        this.dateTimePattern = timeWindow;
        return this;
    }

    public String getDateTimePattern() {
        return dateTimePattern;
    }

    public IndexDefinition setEnabled(boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public IndexDefinition setIgnoreErrors(boolean ignoreErrors) {
        this.ignoreErrors = ignoreErrors;
        return this;
    }

    public boolean ignoreErrors() {
        return ignoreErrors;
    }

    public IndexDefinition setSwitchAliases(boolean switchAliases) {
        this.switchAliases = switchAliases;
        return this;
    }

    public boolean isSwitchAliases() {
        return switchAliases;
    }

    public IndexDefinition setForceMerge(boolean hasForceMerge) {
        this.hasForceMerge = hasForceMerge;
        return this;
    }

    public boolean hasForceMerge() {
        return hasForceMerge;
    }

    public IndexDefinition setReplicaLevel(int replicaLevel) {
        this.replicaLevel = replicaLevel;
        return this;
    }

    public int getReplicaLevel() {
        return replicaLevel;
    }

    public IndexDefinition setRetention(IndexRetention indexRetention) {
        this.indexRetention = indexRetention;
        return this;
    }

    public IndexRetention getRetention() {
        return indexRetention;
    }

}
