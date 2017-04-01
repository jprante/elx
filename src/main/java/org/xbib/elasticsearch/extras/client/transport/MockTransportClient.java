package org.xbib.elasticsearch.extras.client.transport;

import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.xbib.elasticsearch.extras.client.BulkControl;
import org.xbib.elasticsearch.extras.client.BulkMetric;

import java.io.IOException;
import java.util.Map;

/**
 * Mock client, it does not perform actions on a cluster.
 * Useful for testing or dry runs.
 */
public class MockTransportClient extends BulkTransportClient {

    @Override
    public ElasticsearchClient client() {
        return null;
    }

    @Override
    public MockTransportClient init(ElasticsearchClient client, BulkMetric metric, BulkControl control) {
        return this;
    }

    @Override
    public MockTransportClient init(Settings settings, BulkMetric metric, BulkControl control) {
        return this;
    }

    @Override
    public MockTransportClient maxActionsPerRequest(int maxActions) {
        return this;
    }

    @Override
    public MockTransportClient maxConcurrentRequests(int maxConcurrentRequests) {
        return this;
    }

    @Override
    public MockTransportClient maxVolumePerRequest(ByteSizeValue maxVolumePerRequest) {
        return this;
    }

    @Override
    public MockTransportClient flushIngestInterval(TimeValue interval) {
        return this;
    }

    @Override
    public MockTransportClient index(String index, String type, String id, String source) {
        return this;
    }

    @Override
    public MockTransportClient delete(String index, String type, String id) {
        return this;
    }

    @Override
    public MockTransportClient update(String index, String type, String id, String source) {
        return this;
    }

    @Override
    public MockTransportClient bulkIndex(IndexRequest indexRequest) {
        return this;
    }

    @Override
    public MockTransportClient bulkDelete(DeleteRequest deleteRequest) {
        return this;
    }

    @Override
    public MockTransportClient bulkUpdate(UpdateRequest updateRequest) {
        return this;
    }

    @Override
    public MockTransportClient flushIngest() {
        return this;
    }

    @Override
    public MockTransportClient waitForResponses(TimeValue timeValue) throws InterruptedException {
        return this;
    }

    @Override
    public MockTransportClient startBulk(String index, long startRefreshInterval, long stopRefreshIterval) {
        return this;
    }

    @Override
    public MockTransportClient stopBulk(String index) {
        return this;
    }

    @Override
    public MockTransportClient deleteIndex(String index) {
        return this;
    }

    @Override
    public MockTransportClient newIndex(String index) {
        return this;
    }

    @Override
    public MockTransportClient newMapping(String index, String type, Map<String, Object> mapping) {
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
