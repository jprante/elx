package org.xbib.elx.common;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.common.xcontent.yaml.YamlXContent;
import org.xbib.elx.api.AdminClient;
import org.xbib.elx.api.IndexDefinition;
import org.xbib.elx.api.IndexRetention;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.MalformedInputException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

public class DefaultIndexDefinition implements IndexDefinition {

    private String index;

    private String type;

    private String fullIndexName;

    private DateTimeFormatter formatter;

    private Pattern pattern;

    private String settings;

    private String mappings;

    private boolean enabled;

    private boolean ignoreErrors;

    private boolean shift;

    private boolean prune;

    private boolean forcemerge;

    private int replicaLevel;

    private IndexRetention indexRetention;

    private long maxWaitTime;

    private TimeUnit maxWaitTimeUnit;

    private int startRefreshInterval;

    private int stopRefreshInterval;

    public DefaultIndexDefinition(String index, String type) {
        setIndex(index);
        setType(type);
        setDateTimeFormatter(DateTimeFormatter.ofPattern("yyyyMMdd", Locale.getDefault()));
        setDateTimePattern(Pattern.compile("^(.*?)(\\d+)$"));
        setFullIndexName(index + getDateTimeFormatter().format(LocalDate.now()));
        setEnabled(true);
        setMaxWaitTime(Parameters.MAX_WAIT_BULK_RESPONSE_SECONDS.getInteger(), TimeUnit.SECONDS);
    }

    public DefaultIndexDefinition(AdminClient adminClient, String index, String type, Settings settings)
            throws IOException {
        boolean isEnabled = settings.getAsBoolean("enabled", true);
        String indexName = settings.get("name", index);
        String dateTimePatternStr = settings.get("dateTimePattern", "^(.*?)(\\\\d+)$");
        Pattern dateTimePattern = Pattern.compile(dateTimePatternStr);
        String dateTimeFormat = settings.get("dateTimeFormat", "yyyyMMdd");
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(dateTimeFormat)
                .withZone(ZoneId.systemDefault());
        String fullName = indexName + dateTimeFormatter.format(LocalDateTime.now());
        String fullIndexName = adminClient.resolveAlias(fullName).stream().findFirst().orElse(fullName);
        IndexRetention indexRetention = new DefaultIndexRetention()
                .setMinToKeep(settings.getAsInt("retention.mintokeep", 0))
                .setDelta(settings.getAsInt("retention.delta", 0));
        setEnabled(isEnabled)
        .setIndex(indexName)
        .setType(type)
        .setFullIndexName(fullIndexName)
        .setSettings(findSettingsFrom(settings.get("settings")))
        .setMappings(findMappingsFrom(settings.get("mapping")))
        .setDateTimeFormatter(dateTimeFormatter)
        .setDateTimePattern(dateTimePattern)
        .setIgnoreErrors(settings.getAsBoolean("skiperrors", false))
        .setShift(settings.getAsBoolean("shift", false))
        .setPrune(settings.getAsBoolean("prune", false))
        .setReplicaLevel(settings.getAsInt("replica", 0))
        .setMaxWaitTime(settings.getAsLong("timeout", 30L), TimeUnit.SECONDS)
        .setRetention(indexRetention)
        .setStartBulkRefreshSeconds(settings.getAsInt(Parameters.START_BULK_REFRESH_SECONDS.getName(), -1))
        .setStopBulkRefreshSeconds(settings.getAsInt(Parameters.STOP_BULK_REFRESH_SECONDS.getName(), -1));
    }

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
    public IndexDefinition setType(String type) {
        this.type = type;
        return this;
    }

    @Override
    public String getType() {
        return type;
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
    public IndexDefinition setDateTimeFormatter(DateTimeFormatter formatter) {
        this.formatter = formatter;
        return this;
    }

    @Override
    public DateTimeFormatter getDateTimeFormatter() {
        return formatter;
    }

    @Override
    public IndexDefinition setDateTimePattern(Pattern pattern) {
        this.pattern = pattern;
        return this;
    }

    @Override
    public Pattern getDateTimePattern() {
        return pattern;
    }

    @Override
    public IndexDefinition setStartBulkRefreshSeconds(int seconds) {
        this.startRefreshInterval = seconds;
        return this;
    }

    @Override
    public int getStartBulkRefreshSeconds() {
        return startRefreshInterval;
    }

    @Override
    public IndexDefinition setStopBulkRefreshSeconds(int seconds) {
        this.stopRefreshInterval = seconds;
        return this;
    }

    @Override
    public int getStopBulkRefreshSeconds() {
        return stopRefreshInterval;
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
    public IndexDefinition setShift(boolean shift) {
        this.shift = shift;
        return this;
    }

    @Override
    public boolean isShiftEnabled() {
        return shift;
    }

    @Override
    public IndexDefinition setPrune(boolean prune) {
        this.prune = prune;
        return this;
    }

    @Override
    public boolean isPruneEnabled() {
        return prune;
    }

    @Override
    public IndexDefinition setForceMerge(boolean forcemerge) {
        this.forcemerge = forcemerge;
        return this;
    }

    @Override
    public boolean isForceMergeEnabled() {
        return forcemerge;
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


    private static String findSettingsFrom(String string) throws IOException {
        if (string == null) {
            return null;
        }
        try {
            XContentBuilder builder = JsonXContent.contentBuilder();
            try (InputStream inputStream = findInputStream(string)) {
                if (inputStream != null) {
                    Settings settings = Settings.builder().loadFromStream(string, inputStream).build();
                    builder.startObject();
                    settings.toXContent(builder, ToXContent.EMPTY_PARAMS);
                    builder.endObject();
                }
            }
            return builder.string();
        } catch (MalformedURLException e) {
            return string;
        } catch (IOException e) {
            throw new IOException("unable to read JSON from " + string + ": " + e.getMessage(), e);
        }
    }

    private static String findMappingsFrom(String string) throws IOException {
        if (string == null) {
            return null;
        }
        try {
            XContentBuilder builder = JsonXContent.contentBuilder();
            try (InputStream inputStream = findInputStream(string)) {
                if (inputStream != null) {
                    if (string.endsWith(".json")) {
                        Map<String, ?> mappings = JsonXContent.jsonXContent.createParser(inputStream).mapOrdered();
                        builder.map(mappings);
                    }
                    if (string.endsWith(".yml") || string.endsWith(".yaml")) {
                        Map<String, ?> mappings = YamlXContent.yamlXContent.createParser(inputStream).mapOrdered();
                        builder.map(mappings);
                    }
                }
            }
            return builder.string();
        } catch (MalformedInputException e) {
            return string;
        } catch (IOException e) {
            throw new IOException("unable to read JSON from " + string + ": " + e.getMessage(), e);
        }
    }

    private static InputStream findInputStream(String string) {
        if (string == null) {
            return null;
        }
        try {
            URL url = ClassLoader.getSystemClassLoader().getResource(string);
            if (url == null) {
                url = new URL(string);
            }
            return url.openStream();
        } catch (IOException e) {
            return null;
        }
    }
}
