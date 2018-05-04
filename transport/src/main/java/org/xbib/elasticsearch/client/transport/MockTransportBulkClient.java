package org.xbib.elasticsearch.client.transport;

import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.xbib.elasticsearch.client.BulkControl;
import org.xbib.elasticsearch.client.BulkMetric;

import java.io.IOException;
import java.util.Map;

/**
 * Mock client, it does not perform actions on a cluster.
 * Useful for testing or dry runs.
 */
public class MockTransportBulkClient extends TransportBulkClient {

    @Override
    public ElasticsearchClient client() {
        return null;
    }

    @Override
    public MockTransportBulkClient init(ElasticsearchClient client, Settings settings, BulkMetric metric, BulkControl control) {
        return this;
    }

    @Override
    public MockTransportBulkClient maxActionsPerRequest(int maxActions) {
        return this;
    }

    @Override
    public MockTransportBulkClient maxConcurrentRequests(int maxConcurrentRequests) {
        return this;
    }

    @Override
    public MockTransportBulkClient maxVolumePerRequest(ByteSizeValue maxVolumePerRequest) {
        return this;
    }

    @Override
    public MockTransportBulkClient flushIngestInterval(TimeValue interval) {
        return this;
    }

    @Override
    public MockTransportBulkClient index(String index, String type, String id, boolean create, String source) {
        return this;
    }

    @Override
    public MockTransportBulkClient delete(String index, String type, String id) {
        return this;
    }

    @Override
    public MockTransportBulkClient update(String index, String type, String id, String source) {
        return this;
    }

    @Override
    public MockTransportBulkClient indexRequest(IndexRequest indexRequest) {
        return this;
    }

    @Override
    public MockTransportBulkClient deleteRequest(DeleteRequest deleteRequest) {
        return this;
    }

    @Override
    public MockTransportBulkClient updateRequest(UpdateRequest updateRequest) {
        return this;
    }

    @Override
    public MockTransportBulkClient flushIngest() {
        return this;
    }

    @Override
    public MockTransportBulkClient waitForResponses(TimeValue timeValue) throws InterruptedException {
        return this;
    }

    @Override
    public MockTransportBulkClient startBulk(String index, long startRefreshInterval, long stopRefreshIterval) {
        return this;
    }

    @Override
    public MockTransportBulkClient stopBulk(String index) {
        return this;
    }

    @Override
    public MockTransportBulkClient deleteIndex(String index) {
        return this;
    }

    @Override
    public MockTransportBulkClient newIndex(String index) {
        return this;
    }

    @Override
    public MockTransportBulkClient newMapping(String index, String type, Map<String, Object> mapping) {
        return this;
    }

    @Override
    public void putMapping(String index) {
        // mockup method
    }

    @Override
    public void refreshIndex(String index) {
        // mockup method
    }

    @Override
    public void flushIndex(String index) {
        // mockup method
    }

    @Override
    public void waitForCluster(String healthColor, TimeValue timeValue) throws IOException {
        // mockup method
    }

    @Override
    public int waitForRecovery(String index) throws IOException {
        return -1;
    }

    @Override
    public int updateReplicaLevel(String index, int level) throws IOException {
        return -1;
    }

    @Override
    public void shutdown() {
        // mockup method
    }
}
