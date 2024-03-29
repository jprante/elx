package org.xbib.elx.common;

import java.util.logging.Level;
import java.util.logging.Logger;
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
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.xbib.elx.api.BulkClient;
import org.xbib.elx.api.BulkProcessor;
import org.xbib.elx.api.IndexDefinition;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

import static org.xbib.elx.api.IndexDefinition.TYPE_NAME;

public abstract class AbstractBulkClient extends AbstractBasicClient implements BulkClient {

    private static final Logger logger = Logger.getLogger(AbstractBulkClient.class.getName());

    private BulkProcessor bulkProcessor;

    public AbstractBulkClient() {
        super();
    }

    @Override
    public boolean init(Settings settings, String info) throws IOException {
        if (super.init(settings, info)) {
            bulkProcessor = new DefaultBulkProcessor(this, settings);
            return true;
        }
        return false;
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
        if (!bulkProcessor.isClosed()) {
            logger.info("closing bulk processor");
            ensureClientIsPresent();
            bulkProcessor.close();
        }
        super.close();
    }

    @Override
    public void newIndex(IndexDefinition indexDefinition) {
        if (isIndexDefinitionDisabled(indexDefinition)) {
            return;
        }
        String index = indexDefinition.getFullIndexName();
        if (index == null) {
            throw new IllegalArgumentException("no index name given");
        }
        ensureClientIsPresent();
        CreateIndexRequestBuilder createIndexRequestBuilder =
                new CreateIndexRequestBuilder(client, CreateIndexAction.INSTANCE)
                .setIndex(index);
        if (indexDefinition.getSettings() == null) {
            try {
                XContentBuilder builder = JsonXContent.contentBuilder()
                        .startObject()
                        .startObject("index")
                        .field("number_of_shards", indexDefinition.getShardCount())
                        .field("number_of_replicas", 0) // always 0
                        .endObject()
                        .endObject();
                indexDefinition.setSettings(Strings.toString(builder));
            } catch (IOException e) {
                logger.log(Level.WARNING, e.getMessage(), e);
            }
        }
        Settings settings = Settings.builder()
                .loadFromSource(indexDefinition.getSettings(), XContentType.JSON)
                .put("index.number_of_shards", indexDefinition.getShardCount())
                .put("index.number_of_replicas", 0) // always 0
                .build();
        createIndexRequestBuilder.setSettings(settings);
        try {
            if (indexDefinition.getMappings() != null) {
                createIndexRequestBuilder.addMapping(TYPE_NAME, indexDefinition.getMappings());
            } else {
                XContentBuilder builder = JsonXContent.contentBuilder()
                        .startObject().startObject(TYPE_NAME).endObject().endObject();
                createIndexRequestBuilder.addMapping(TYPE_NAME, builder);
            }
        } catch (IOException e) {
            logger.log(Level.WARNING, e.getMessage(), e);
        }
        CreateIndexResponse createIndexResponse = createIndexRequestBuilder.execute().actionGet();
        if (createIndexResponse.isAcknowledged()) {
            logger.log(Level.INFO, "index created: " + index);
        } else {
            logger.log(Level.WARNING, "index creation of {} not acknowledged", index);
            return;
        }
        // we really need state GREEN. If yellow, we may trigger shard write errors and queue will exceed quickly.
        waitForHealthyCluster();
    }

    @Override
    public void startBulk(IndexDefinition indexDefinition) {
        if (isIndexDefinitionDisabled(indexDefinition)) {
            return;
        }
        ensureClientIsPresent();
        String indexName = indexDefinition.getFullIndexName();
        int interval = indexDefinition.getStartBulkRefreshSeconds();
        if (interval != 0) {
            logger.log(Level.INFO, "starting bulk on " + indexName + " with new refresh interval " + interval);
            updateIndexSetting(indexName,
                    "refresh_interval", interval >=0 ? interval + "s" : interval, 30L, TimeUnit.SECONDS);
            updateIndexSetting(indexName,
                    "index.translog.durability", "async", 30L, TimeUnit.SECONDS);
        } else {
            logger.log(Level.WARNING, "ignoring starting bulk on " + indexName + " with refresh interval " + interval);
        }
    }

    @Override
    public void stopBulk(IndexDefinition indexDefinition) {
        if (isIndexDefinitionDisabled(indexDefinition)) {
            return;
        }
        if (bulkProcessor != null) {
            ensureClientIsPresent();
            String indexName = indexDefinition.getFullIndexName();
            int interval = indexDefinition.getStopBulkRefreshSeconds();
            try {
                logger.info("flushing bulk");
                bulkProcessor.flush();
            } catch (IOException e) {
                // can never happen
            }
            if (bulkProcessor.waitForBulkResponses(60L, TimeUnit.SECONDS)) {
                if (interval != 0) {
                    logger.info("stopping bulk on " + indexName + " with new refresh interval " + interval);
                    updateIndexSetting(indexName, "refresh_interval",
                            interval >= 0 ? interval + "s" : interval, 30L, TimeUnit.SECONDS);
                    updateIndexSetting(indexName,
                            "index.translog.durability", "request", 30L, TimeUnit.SECONDS);
                } else {
                    logger.log(Level.WARNING, "ignoring stopping bulk on " + indexName + " with refresh interval " + interval);
                }
            }
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
                .id(id).create(create).source(source, XContentType.JSON));
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
        return delete(new DeleteRequest().index(indexDefinition.getFullIndexName()).id(id));
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
                .id(id).doc(source, XContentType.JSON));
    }

    @Override
    public BulkClient update(UpdateRequest updateRequest) {
        if (bulkProcessor != null) {
            ensureClientIsPresent();
            bulkProcessor.add(updateRequest);
        }
        return this;
    }

    @Override
    public boolean waitForResponses(long timeout, TimeUnit timeUnit) {
        if (bulkProcessor != null) {
            ensureClientIsPresent();
            return bulkProcessor.waitForBulkResponses(timeout, timeUnit);
        }
        return false;
    }

    @Override
    public void updateIndexSetting(String index, String key, Object value, long timeout, TimeUnit timeUnit) {
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
