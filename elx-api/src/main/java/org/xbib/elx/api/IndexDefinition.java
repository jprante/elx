package org.xbib.elx.api;

import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

public interface IndexDefinition {

    void setIndex(String index);

    String getIndex();

    void setType(String type);

    String getType();

    void setFullIndexName(String fullIndexName);

    String getFullIndexName();

    void setSettings(String settings);

    String getSettings();

    void setMappings(String mappings);

    Map<String, Object> getMappings();

    Set<String> getMappingFields();

    void setDateTimeFormatter(DateTimeFormatter formatter);

    DateTimeFormatter getDateTimeFormatter();

    void setDateTimePattern(Pattern pattern);

    Pattern getDateTimePattern();

    void setStartBulkRefreshSeconds(int seconds);

    int getStartBulkRefreshSeconds();

    void setStopBulkRefreshSeconds(int seconds);

    int getStopBulkRefreshSeconds();

    void setEnabled(boolean enabled);

    boolean isEnabled();

    void setShift(boolean shift);

    boolean isShiftEnabled();

    void setPrune(boolean prune);

    boolean isPruneEnabled();

    void setForceMerge(boolean forcemerge);

    boolean isForceMergeEnabled();

    void setShardCount(int shardCount);

    int getShardCount();

    void setReplicaCount(int replicaCount);

    int getReplicaCount();

    void setDelta(int delta);

    int getDelta();

    void setMinToKeep(int minToKeep);

    int getMinToKeep();
}
