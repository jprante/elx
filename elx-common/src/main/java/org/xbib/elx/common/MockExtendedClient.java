package org.xbib.elx.common;

import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.settings.Settings;

import java.util.Map;

/**
 * Mock client, it does not perform actions on a cluster. Useful for testing or dry runs.
 */
public class MockExtendedClient extends AbstractExtendedClient {

    @Override
    public ElasticsearchClient getClient() {
        return null;
    }

    @Override
    public MockExtendedClient init(Settings settings) {
        return this;
    }

    @Override
    protected ElasticsearchClient createClient(Settings settings) {
        return null;
    }

    @Override
    public MockExtendedClient maxActionsPerRequest(int maxActions) {
        return this;
    }

    @Override
    public MockExtendedClient maxConcurrentRequests(int maxConcurrentRequests) {
        return this;
    }

    @Override
    public MockExtendedClient maxVolumePerRequest(String maxVolumePerRequest) {
        return this;
    }

    @Override
    public MockExtendedClient flushIngestInterval(String interval) {
        return this;
    }

    @Override
    public MockExtendedClient index(String index, String type, String id, boolean create, String source) {
        return this;
    }

    @Override
    public MockExtendedClient delete(String index, String type, String id) {
        return this;
    }

    @Override
    public MockExtendedClient update(String index, String type, String id, String source) {
        return this;
    }

    @Override
    public MockExtendedClient indexRequest(IndexRequest indexRequest) {
        return this;
    }

    @Override
    public MockExtendedClient deleteRequest(DeleteRequest deleteRequest) {
        return this;
    }

    @Override
    public MockExtendedClient updateRequest(UpdateRequest updateRequest) {
        return this;
    }

    @Override
    public MockExtendedClient flushIngest() {
        return this;
    }

    @Override
    public MockExtendedClient waitForResponses(String timeValue) {
        return this;
    }

    @Override
    public MockExtendedClient startBulk(String index, long startRefreshInterval, long stopRefreshIterval) {
        return this;
    }

    @Override
    public MockExtendedClient stopBulk(String index) {
        return this;
    }

    @Override
    public MockExtendedClient deleteIndex(String index) {
        return this;
    }

    @Override
    public MockExtendedClient newIndex(String index) {
        return this;
    }

    @Override
    public MockExtendedClient newMapping(String index, String type, Map<String, Object> mapping) {
        return this;
    }

    @Override
    public void putMapping(String index) {
    }

    @Override
    public void refreshIndex(String index) {
    }

    @Override
    public void flushIndex(String index) {
    }

    @Override
    public void waitForCluster(String healthColor, String timeValue) {
    }

    @Override
    public int waitForRecovery(String index) {
        return -1;
    }

    @Override
    public int updateReplicaLevel(String index, int level) {
        return -1;
    }

    @Override
    public void shutdown() {
        // nothing to do
    }
}
