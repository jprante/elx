package org.xbib.elx.common;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.indices.create.CreateIndexAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.flush.FlushAction;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshAction;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.xbib.elx.api.BulkClient;
import org.xbib.elx.api.BulkProcessor;
import org.xbib.elx.api.IndexDefinition;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class AbstractBulkClient extends AbstractBasicClient implements BulkClient {

    private static final Logger logger = LogManager.getLogger(AbstractBulkClient.class.getName());

    private BulkProcessor bulkProcessor;

    private final AtomicBoolean closed;

    public AbstractBulkClient() {
        super();
        closed = new AtomicBoolean(true);
    }

    @Override
    public void init(Settings settings) throws IOException {
        if (closed.compareAndSet(true, false)) {
            super.init(settings);
            bulkProcessor = new DefaultBulkProcessor(this, settings);
        }
    }

    @Override
    public BulkProcessor getBulkProcessor() {
        return bulkProcessor;
    }

    @Override
    public void flush() throws IOException {
        if (bulkProcessor != null) {
            bulkProcessor.flush();
        }
    }

    @Override
    public void close() throws IOException {
        if (closed.compareAndSet(false, true)) {
            ensureClientIsPresent();
            if (bulkProcessor != null) {
                logger.info("closing bulk procesor");
                bulkProcessor.close();
            }
            closeClient(settings);
            super.close();
        }
    }

    @Override
    public void newIndex(IndexDefinition indexDefinition) throws IOException {
        if (isIndexDefinitionDisabled(indexDefinition)) {
            return;
        }
        String index = indexDefinition.getFullIndexName();
        if (index == null) {
            throw new IllegalArgumentException("no index name given");
        }
        String type = indexDefinition.getType();
        if (type == null) {
            throw new IllegalArgumentException("no index type given");
        }
        ensureClientIsPresent();
        CreateIndexRequestBuilder createIndexRequestBuilder = new CreateIndexRequestBuilder(client, CreateIndexAction.INSTANCE)
                .setIndex(index);
        if (indexDefinition.getSettings() == null) {
            XContentBuilder builder = JsonXContent.contentBuilder()
                    .startObject()
                    .startObject("index")
                    .field("number_of_shards", 1)
                    .field("number_of_replicas", 0)
                    .endObject()
                    .endObject();
            indexDefinition.setSettings(builder.string());
        }
        Settings settings = Settings.builder().loadFromSource(indexDefinition.getSettings()).build();
        createIndexRequestBuilder.setSettings(settings);
        if (indexDefinition.getMappings() != null) {
            Map<String, Object> mappings = JsonXContent.jsonXContent.createParser(indexDefinition.getMappings()).mapOrdered();
            createIndexRequestBuilder.addMapping(type, mappings);
        } else {
            XContentBuilder builder = JsonXContent.contentBuilder().startObject().startObject(type).endObject().endObject();
            createIndexRequestBuilder.addMapping(type, builder);
        }
        CreateIndexResponse createIndexResponse = createIndexRequestBuilder.execute().actionGet();
        if (createIndexResponse.isAcknowledged()) {
            logger.info("index {} created", index);
        } else {
            logger.warn("index creation of {} not acknowledged", index);
            return;
        }
        // we really need state GREEN. If yellow, we may trigger shard write errors and queue will exceed quickly.
        waitForCluster("GREEN", 300L, TimeUnit.SECONDS);
    }

    @Override
    public void startBulk(IndexDefinition indexDefinition) throws IOException {
        if (isIndexDefinitionDisabled(indexDefinition)) {
            return;
        }
        if (bulkProcessor != null) {
            ensureClientIsPresent();
            Long bulkQueueSize = getThreadPoolQueueSize("bulk");
            if (bulkQueueSize != null && bulkQueueSize <= 64) {
                logger.info("found bulk queue size " + bulkQueueSize + ", expanding to " + bulkQueueSize * 4);
                bulkQueueSize = bulkQueueSize * 4;
            } else {
                logger.warn("undefined or small bulk queue size found: " + bulkQueueSize + " assuming 256");
                bulkQueueSize = 256L;
            }
            putClusterSetting("threadpool.bulk.queue_size", bulkQueueSize, 30L, TimeUnit.SECONDS);
            bulkProcessor.startBulkMode(indexDefinition);
        }
    }

    @Override
    public void stopBulk(IndexDefinition indexDefinition) throws IOException {
        if (isIndexDefinitionDisabled(indexDefinition)) {
            return;
        }
        if (bulkProcessor != null) {
            ensureClientIsPresent();
            bulkProcessor.stopBulkMode(indexDefinition);
        }
    }

    @Override
    public BulkClient index(IndexDefinition indexDefinition, String id, boolean create, String source) {
        return index(indexDefinition, id, create, new BytesArray(source.getBytes(StandardCharsets.UTF_8)));
    }

    @Override
    public BulkClient index(IndexDefinition indexDefinition, String id, boolean create, BytesReference source) {
        if (isIndexDefinitionDisabled(indexDefinition)) {
            return this;
        }
        return index(new IndexRequest()
                .index(indexDefinition.getFullIndexName())
                .type(indexDefinition.getType())
                .id(id).create(create).source(source));
    }

    @Override
    public BulkClient index(IndexRequest indexRequest) {
        if (bulkProcessor != null) {
            ensureClientIsPresent();
            bulkProcessor.add(indexRequest);
        }
        return this;
    }

    @Override
    public BulkClient delete(IndexDefinition indexDefinition, String id) {
        if (isIndexDefinitionDisabled(indexDefinition)) {
            return this;
        }
        return delete(new DeleteRequest()
                .index(indexDefinition.getFullIndexName())
                .type(indexDefinition.getType())
                .id(id));
    }

    @Override
    public BulkClient delete(DeleteRequest deleteRequest) {
        if (bulkProcessor != null) {
            ensureClientIsPresent();
            bulkProcessor.add(deleteRequest);
        }
        return this;
    }

    @Override
    public BulkClient update(IndexDefinition indexDefinition, String id, String source) {
        return update(indexDefinition, id, new BytesArray(source.getBytes(StandardCharsets.UTF_8)));
    }

    @Override
    public BulkClient update(IndexDefinition indexDefinition, String id, BytesReference source) {
        if (isIndexDefinitionDisabled(indexDefinition)) {
            return this;
        }
        return update(new UpdateRequest()
                .index(indexDefinition.getFullIndexName())
                .type(indexDefinition.getType())
                .id(id).doc(source.hasArray() ? source.array() : source.toBytes()));
    }

    @Override
    public BulkClient update(UpdateRequest updateRequest) {
        ensureClientIsPresent();
        bulkProcessor.add(updateRequest);
        return this;
    }

    @Override
    public boolean waitForResponses(long timeout, TimeUnit timeUnit) {
        if (bulkProcessor != null) {
            ensureClientIsPresent();
            return bulkProcessor.waitForBulkResponses(timeout, timeUnit);
        }
        return true;
    }

    @Override
    public void updateIndexSetting(String index, String key, Object value, long timeout, TimeUnit timeUnit) throws IOException {
        super.updateIndexSetting(index, key, value, timeout, timeUnit);
    }

    @Override
    public void flushIndex(IndexDefinition indexDefinition) {
        if (isIndexDefinitionDisabled(indexDefinition)) {
            return;
        }
        ensureClientIsPresent();
        client.execute(FlushAction.INSTANCE, new FlushRequest(indexDefinition.getFullIndexName())).actionGet();
    }

    @Override
    public void refreshIndex(IndexDefinition indexDefinition) {
        if (isIndexDefinitionDisabled(indexDefinition)) {
            return;
        }
        ensureClientIsPresent();
        client.execute(RefreshAction.INSTANCE, new RefreshRequest(indexDefinition.getFullIndexName())).actionGet();
    }
}
