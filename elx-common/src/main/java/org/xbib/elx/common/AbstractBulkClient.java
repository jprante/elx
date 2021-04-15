package org.xbib.elx.common;

import org.apache.logging.log4j.Level;
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
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.xbib.elx.api.BulkClient;
import org.xbib.elx.api.BulkController;
import org.xbib.elx.api.IndexDefinition;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.xbib.elx.api.IndexDefinition.TYPE_NAME;

public abstract class AbstractBulkClient extends AbstractBasicClient implements BulkClient {

    private static final Logger logger = LogManager.getLogger(AbstractBulkClient.class.getName());

    private BulkController bulkController;

    private final AtomicBoolean closed = new AtomicBoolean(true);

    @Override
    public void init(Settings settings) throws IOException {
        if (closed.compareAndSet(true, false)) {
            super.init(settings);
            logger.log(Level.INFO, "initializing bulk controller with settings = " + settings.toDelimitedString(','));
            bulkController = new DefaultBulkController(this);
            bulkController.init(settings);
        }
    }

    @Override
    public BulkController getBulkController() {
        return bulkController;
    }

    @Override
    public void flush() throws IOException {
        if (bulkController != null) {
            bulkController.flush();
        }
    }

    @Override
    public void close() throws IOException {
        if (closed.compareAndSet(false, true)) {
            ensureClientIsPresent();
            if (bulkController != null) {
                logger.info("closing bulk controller");
                bulkController.close();
            }
            closeClient(settings);
        }
    }

    @Override
    public void newIndex(IndexDefinition indexDefinition) throws IOException {
        if (!ensureIndexDefinition(indexDefinition)) {
            return;
        }
        String index = indexDefinition.getFullIndexName();
        if (index == null) {
            throw new IllegalArgumentException("no index name given");
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
            indexDefinition.setSettings(Strings.toString(builder));
        }
        Settings settings = Settings.builder().loadFromSource(indexDefinition.getSettings(), XContentType.JSON).build();
        createIndexRequestBuilder.setSettings(settings);
        if (indexDefinition.getMappings() != null) {
            Map<String, Object> mappings = JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY,
                            DeprecationHandler.THROW_UNSUPPORTED_OPERATION, indexDefinition.getMappings()).mapOrdered();
            createIndexRequestBuilder.addMapping(TYPE_NAME, mappings);
        } else {
            XContentBuilder builder = JsonXContent.contentBuilder().startObject().startObject(TYPE_NAME).endObject().endObject();
            createIndexRequestBuilder.addMapping(TYPE_NAME, builder);
        }
        CreateIndexResponse createIndexResponse = createIndexRequestBuilder.execute().actionGet();
        if (createIndexResponse.isAcknowledged()) {
            logger.info("index {} created", index);
        } else {
            logger.warn("index creation of {} not acknowledged", index);
            return;
        }
        // we really need state GREEN. If yellow, we may trigger shard write errors and queue will exceed quickly.
        logger.info("waiting for GREEN after index {} was created", index);
        waitForCluster("GREEN", indexDefinition.getMaxWaitTime(), indexDefinition.getMaxWaitTimeUnit());
    }

    @Override
    public void startBulk(IndexDefinition indexDefinition) throws IOException {
        if (!ensureIndexDefinition(indexDefinition)) {
            return;
        }
        if (bulkController != null) {
            ensureClientIsPresent();
            bulkController.startBulkMode(indexDefinition);
        }
    }

    @Override
    public void stopBulk(IndexDefinition indexDefinition) throws IOException {
        if (!ensureIndexDefinition(indexDefinition)) {
            return;
        }
        if (bulkController != null) {
            ensureClientIsPresent();
            bulkController.stopBulkMode(indexDefinition);
        }
    }

    @Override
    public BulkClient index(IndexDefinition indexDefinition, String id, boolean create, String source) {
        return index(indexDefinition, id, create, new BytesArray(source.getBytes(StandardCharsets.UTF_8)));
    }

    @Override
    public BulkClient index(IndexDefinition indexDefinition, String id, boolean create, BytesReference source) {
        if (!ensureIndexDefinition(indexDefinition)) {
            return this;
        }
        return index(new IndexRequest()
                .index(indexDefinition.getFullIndexName())
                .id(id).create(create).source(source, XContentType.JSON));
    }

    @Override
    public BulkClient index(IndexRequest indexRequest) {
        if (bulkController != null) {
            ensureClientIsPresent();
            bulkController.bulkIndex(indexRequest);
        }
        return this;
    }

    @Override
    public BulkClient delete(IndexDefinition indexDefinition, String id) {
        if (!ensureIndexDefinition(indexDefinition)) {
            return this;
        }
        return delete(new DeleteRequest().index(indexDefinition.getFullIndexName()).id(id));
    }

    @Override
    public BulkClient delete(DeleteRequest deleteRequest) {
        if (bulkController != null) {
            ensureClientIsPresent();
            bulkController.bulkDelete(deleteRequest);
        }
        return this;
    }

    @Override
    public BulkClient update(IndexDefinition indexDefinition, String id, String source) {
        return update(indexDefinition, id, new BytesArray(source.getBytes(StandardCharsets.UTF_8)));
    }

    @Override
    public BulkClient update(IndexDefinition indexDefinition, String id, BytesReference source) {
        if (!ensureIndexDefinition(indexDefinition)) {
            return this;
        }
        return update(new UpdateRequest()
                .index(indexDefinition.getFullIndexName())
                .id(id).doc(source, XContentType.JSON));
    }

    @Override
    public BulkClient update(UpdateRequest updateRequest) {
        if (bulkController != null) {
            ensureClientIsPresent();
            bulkController.bulkUpdate(updateRequest);
        }
        return this;
    }

    @Override
    public boolean waitForResponses(long timeout, TimeUnit timeUnit) {
        ensureClientIsPresent();
        return bulkController.waitForBulkResponses(timeout, timeUnit);
    }

    @Override
    public void updateIndexSetting(String index, String key, Object value, long timeout, TimeUnit timeUnit) throws IOException {
        super.updateIndexSetting(index, key, value, timeout, timeUnit);
    }

    @Override
    public void flushIndex(IndexDefinition indexDefinition) {
        if (!ensureIndexDefinition(indexDefinition)) {
            return;
        }
        ensureClientIsPresent();
        client.execute(FlushAction.INSTANCE, new FlushRequest(indexDefinition.getFullIndexName())).actionGet();
    }

    @Override
    public void refreshIndex(IndexDefinition indexDefinition) {
        if (!ensureIndexDefinition(indexDefinition)) {
            return;
        }
        ensureClientIsPresent();
        client.execute(RefreshAction.INSTANCE, new RefreshRequest(indexDefinition.getFullIndexName())).actionGet();
    }
}
