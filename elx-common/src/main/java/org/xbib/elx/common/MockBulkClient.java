package org.xbib.elx.common;

import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.xbib.elx.api.BulkClient;
import org.xbib.elx.api.BulkController;
import org.xbib.elx.api.BulkMetric;
import org.xbib.elx.api.IndexDefinition;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * A mocked client, it does not perform any actions on a cluster. Useful for testing.
 */
public class MockBulkClient extends MockNativeClient implements BulkClient {

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
    protected ElasticsearchClient createClient(Settings settings) {
        return null;
    }

    @Override
    protected void closeClient() {
    }

    @Override
    public BulkMetric getBulkMetric() {
        return null;
    }

    @Override
    public BulkController getBulkController() {
        return null;
    }

    @Override
    public void newIndex(String index) throws IOException {

    }

    @Override
    public void newIndex(IndexDefinition indexDefinition) throws IOException {

    }

    @Override
    public void newIndex(String index, Settings settings) throws IOException {

    }

    @Override
    public void newIndex(String index, Settings settings, XContentBuilder mapping) throws IOException {

    }

    @Override
    public void newIndex(String index, Settings settings, Map<String, ?> mapping) throws IOException {

    }

    @Override
    public BulkClient index(String index, String id, boolean create, BytesReference source) {
        return null;
    }

    @Override
    public MockBulkClient index(String index, String id, boolean create, String source) {
        return this;
    }

    @Override
    public MockBulkClient delete(String index, String id) {
        return this;
    }

    @Override
    public MockBulkClient update(String index, String id, String source) {
        return this;
    }

    @Override
    public MockBulkClient index(IndexRequest indexRequest) {
        return this;
    }

    @Override
    public MockBulkClient delete(DeleteRequest deleteRequest) {
        return this;
    }

    @Override
    public BulkClient update(String index, String id, BytesReference source) {
        return null;
    }

    @Override
    public MockBulkClient update(UpdateRequest updateRequest) {
        return this;
    }

    @Override
    public void startBulk(IndexDefinition indexDefinition) throws IOException {

    }

    @Override
    public void startBulk(String index, long startRefreshInterval, long stopRefreshIterval) {
    }

    @Override
    public void stopBulk(IndexDefinition indexDefinition) throws IOException {

    }

    @Override
    public void stopBulk(String index, long maxWaitTime, TimeUnit timeUnit) {
    }

    @Override
    public boolean waitForResponses(long maxWaitTime, TimeUnit timeUnit) {
        return true;
    }

    @Override
    public void updateIndexSetting(String index, String key, Object value, long timeout, TimeUnit timeUnit) throws IOException {

    }

    @Override
    public void refreshIndex(String index) {
    }

    @Override
    public void flushIndex(String index) {
    }


    @Override
    public void flush() {
        // nothing to do
    }

    @Override
    public void close() {
        // nothing to do
    }
}
