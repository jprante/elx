package org.xbib.elx.common;

import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.settings.Settings;
import org.xbib.elx.api.*;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * A mocked client, it does not perform any actions on a cluster. Useful for testing.
 */
public class MockAdminClient extends MockNativeClient implements AdminClient {

    @Override
    public void setClient(ElasticsearchClient client) {

    }

    @Override
    public ElasticsearchClient getClient() {
        return null;
    }

    @Override
    public void init(Settings settings) {
    }

    @Override
    public String getClusterName() {
        return null;
    }

    @Override
    public String getHealthColor(long maxWaitTime, TimeUnit timeUnit) {
        return null;
    }

    @Override
    public IndexDefinition buildIndexDefinitionFromSettings(String index, Settings settings) throws IOException {
        return null;
    }

    @Override
    public Map<String, ?> getMapping(String index) {
        return null;
    }

    @Override
    public Map<String, ?> getMapping(String index, String type) {
        return null;
    }

    @Override
    public AdminClient deleteIndex(IndexDefinition indexDefinition) {
        return this;
    }

    @Override
    public AdminClient deleteIndex(String index) {
        return this;
    }

    @Override
    public AdminClient updateReplicaLevel(IndexDefinition indexDefinition, int level) throws IOException {
        return this;
    }

    @Override
    public boolean forceMerge(String index, long maxWaitTime, TimeUnit timeUnit) {
        return true;
    }

    @Override
    public String resolveAlias(String alias) {
        return null;
    }

    @Override
    public String resolveMostRecentIndex(String alias) {
        return null;
    }

    @Override
    public Map<String, String> getAliases(String index) {
        return null;
    }

    @Override
    public IndexShiftResult shiftIndex(IndexDefinition indexDefinition, List<String> additionalAliases) {
        return null;
    }

    @Override
    public IndexShiftResult shiftIndex(IndexDefinition indexDefinition, List<String> additionalAliases, IndexAliasAdder indexAliasAdder) {
        return null;
    }

    @Override
    public IndexShiftResult shiftIndex(String index, String fullIndexName, List<String> additionalAliases) {
        return null;
    }

    @Override
    public IndexShiftResult shiftIndex(String index, String fullIndexName, List<String> additionalAliases, IndexAliasAdder adder) {
        return null;
    }

    @Override
    public IndexPruneResult pruneIndex(IndexDefinition indexDefinition) {
        return null;
    }

    @Override
    public IndexPruneResult pruneIndex(String index, String fullIndexName, int delta, int mintokeep, boolean perform) {
        return null;
    }

    @Override
    public Long mostRecentDocument(String index, String timestampfieldname) throws IOException {
        return null;
    }

    @Override
    public void waitForCluster(String healthColor, long timeValue, TimeUnit timeUnit) {
    }

    @Override
    public void waitForShards(long maxWaitTime, TimeUnit timeUnit) {

    }

    @Override
    public long getSearchableDocs(String index) {
        return 0;
    }

    @Override
    public boolean isIndexExists(String index) {
        return false;
    }

    @Override
    public AdminClient updateReplicaLevel(String index, int level, long maxWaitTime, TimeUnit timeUnit) {
        return this;
    }

    @Override
    public int getReplicaLevel(IndexDefinition indexDefinition) {
        return 0;
    }

    @Override
    public int getReplicaLevel(String index) {
        return 0;
    }

    @Override
    public boolean forceMerge(IndexDefinition indexDefinition) {
        return false;
    }

    @Override
    public void close() {
        // nothing to do
    }
}
