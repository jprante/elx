package org.xbib.elx.common;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.common.xcontent.yaml.YamlXContent;
import org.xbib.elx.api.AdminClient;
import org.xbib.elx.api.IndexDefinition;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.MalformedInputException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
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

    private boolean shift;

    private boolean prune;

    private boolean forcemerge;

    private int shardCount;

    private int replicaCount;

    private int startRefreshInterval;

    private int stopRefreshInterval;

    private int delta;

    private int minToKeep;

    public DefaultIndexDefinition(String index, String type) {
        setIndex(index);
        setType(type);
        setDateTimeFormatter(DateTimeFormatter.ofPattern("yyyyMMdd", Locale.getDefault()));
        setDateTimePattern(Pattern.compile("^(.*?)(\\d+)$"));
        setFullIndexName(index + getDateTimeFormatter().format(LocalDateTime.now()));
        setShardCount(1);
        setShift(false);
        setPrune(false);
        setForceMerge(false);
        setEnabled(true);
    }

    public DefaultIndexDefinition(AdminClient adminClient, String index, String type, Settings settings)
            throws IOException {
        String indexName = settings.get("name", index);
        String indexType = settings.get("type", type);
        setIndex(indexName);
        setType(indexType);
        boolean enabled = settings.getAsBoolean("enabled", true);
        setEnabled(enabled);
        boolean forcemerge = settings.getAsBoolean("forcemerge", true);
        setForceMerge(forcemerge);
        setShardCount(settings.getAsInt("shards", 1));
        setReplicaCount(settings.getAsInt("replicas", 1));
        String fullIndexName = adminClient.resolveAlias(indexName).stream().findFirst().orElse(indexName);
        setFullIndexName(fullIndexName);
        setStartBulkRefreshSeconds(settings.getAsInt(Parameters.BULK_START_REFRESH_SECONDS.getName(),
                Parameters.BULK_START_REFRESH_SECONDS.getInteger()));
        setStopBulkRefreshSeconds(settings.getAsInt(Parameters.BULK_STOP_REFRESH_SECONDS.getName(),
                Parameters.BULK_STOP_REFRESH_SECONDS.getInteger()));
        if (settings.get("settings") != null && settings.get("mapping") != null) {
            setSettings(findSettingsFrom(settings.get("settings")));
            setMappings(findMappingsFrom(settings.get("mapping")));
        }
        boolean shift = settings.getAsBoolean("shift", false);
        setShift(shift);
        if (shift) {
            String dateTimeFormat = settings.get(Parameters.DATE_TIME_FORMAT.getName(),
                    Parameters.DATE_TIME_FORMAT.getString());
            DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(dateTimeFormat, Locale.getDefault())
                    .withZone(ZoneId.systemDefault());
            setDateTimeFormatter(dateTimeFormatter);
            String dateTimePatternStr = settings.get("dateTimePattern", "^(.*?)(\\d+)$");
            Pattern dateTimePattern = Pattern.compile(dateTimePatternStr);
            setDateTimePattern(dateTimePattern);
            String fullName = indexName + dateTimeFormatter.format(LocalDateTime.now());
            fullIndexName = adminClient.resolveAlias(fullName).stream().findFirst().orElse(fullName);
            setFullIndexName(fullIndexName);
            boolean prune = settings.getAsBoolean("prune", false);
            setPrune(prune);
            if (prune) {
                setMinToKeep(settings.getAsInt("retention.mintokeep", 2));
                setDelta(settings.getAsInt("retention.delta", 2));
            }
        }
    }

    @Override
    public void setIndex(String index) {
        this.index = index;
    }

    @Override
    public String getIndex() {
        return index;
    }

    @Override
    public void setType(String type) {
        this.type = type;
    }

    @Override
    public String getType() {
        return type;
    }

    @Override
    public void setFullIndexName(String fullIndexName) {
        this.fullIndexName = fullIndexName;
    }

    @Override
    public String getFullIndexName() {
        return fullIndexName;
    }

    @Override
    public void setSettings(String settings) {
        this.settings = settings;
    }

    @Override
    public String getSettings() {
        return settings;
    }

    @Override
    public void setMappings(String mappings) {
        this.mappings = mappings;
    }

    @Override
    public Map<String, Object> getMappings() {
        if (mappings == null) {
            return null;
        }
        try {
            return JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY,
                    DeprecationHandler.THROW_UNSUPPORTED_OPERATION, mappings).mapOrdered();
        } catch (IOException e) {
            return null;
        }
    }

    public Set<String> getMappingFields() {
        if (mappings == null) {
            return null;
        }
        try {
            return Settings.fromXContent(JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION, mappings)).getGroups("properties").keySet();
        } catch (IOException e) {
            return null;
        }
    }

    @Override
    public void setDateTimeFormatter(DateTimeFormatter formatter) {
        this.formatter = formatter;
    }

    @Override
    public DateTimeFormatter getDateTimeFormatter() {
        return formatter;
    }

    @Override
    public void setDateTimePattern(Pattern pattern) {
        this.pattern = pattern;
    }

    @Override
    public Pattern getDateTimePattern() {
        return pattern;
    }

    @Override
    public void setStartBulkRefreshSeconds(int seconds) {
        this.startRefreshInterval = seconds;
    }

    @Override
    public int getStartBulkRefreshSeconds() {
        return startRefreshInterval;
    }

    @Override
    public void setStopBulkRefreshSeconds(int seconds) {
        this.stopRefreshInterval = seconds;
    }

    @Override
    public int getStopBulkRefreshSeconds() {
        return stopRefreshInterval;
    }

    @Override
    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    @Override
    public boolean isEnabled() {
        return enabled;
    }

    @Override
    public void setShift(boolean shift) {
        this.shift = shift;
    }

    @Override
    public boolean isShiftEnabled() {
        return shift;
    }

    @Override
    public void setPrune(boolean prune) {
        this.prune = prune;
    }

    @Override
    public boolean isPruneEnabled() {
        return prune;
    }

    @Override
    public void setForceMerge(boolean forcemerge) {
        this.forcemerge = forcemerge;
    }

    @Override
    public boolean isForceMergeEnabled() {
        return forcemerge;
    }

    @Override
    public void setShardCount(int shardCount) {
        this.shardCount = shardCount;
    }

    @Override
    public int getShardCount() {
        return shardCount;
    }

    @Override
    public void setReplicaCount(int replicaCount) {
        this.replicaCount = replicaCount;
    }

    @Override
    public int getReplicaCount() {
        return replicaCount;
    }

    @Override
    public void setDelta(int delta) {
        this.delta = delta;
    }

    @Override
    public int getDelta() {
        return delta;
    }

    @Override
    public void setMinToKeep(int minToKeep) {
        this.minToKeep = minToKeep;
    }

    @Override
    public int getMinToKeep() {
        return minToKeep;
    }
    private static String findSettingsFrom(String string) throws IOException {
        if (string == null) {
            return null;
        }
        try {
            XContentBuilder builder = JsonXContent.contentBuilder();
            try (InputStream inputStream = findInputStream(string)) {
                if (inputStream != null) {
                    Settings settings = Settings.builder().loadFromStream(string, inputStream, true).build();
                    builder.startObject();
                    settings.toXContent(builder, ToXContent.EMPTY_PARAMS);
                    builder.endObject();
                }
            }
            return Strings.toString(builder);
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
                        Map<String, ?> mappings = JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY,
                                DeprecationHandler.THROW_UNSUPPORTED_OPERATION, inputStream).mapOrdered();
                        builder.map(mappings);
                    }
                    if (string.endsWith(".yml") || string.endsWith(".yaml")) {
                        Map<String, ?> mappings = YamlXContent.yamlXContent.createParser(NamedXContentRegistry.EMPTY,
                                DeprecationHandler.THROW_UNSUPPORTED_OPERATION, inputStream).mapOrdered();
                        builder.map(mappings);
                    }
                }
            }
            return Strings.toString(builder);
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
