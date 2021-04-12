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
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.xbib.elx.api.BulkClient;
import org.xbib.elx.api.BulkController;
import org.xbib.elx.api.IndexDefinition;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class AbstractBulkClient extends AbstractBasicClient implements BulkClient {

    private static final Logger logger = LogManager.getLogger(AbstractBulkClient.class.getName());

    private BulkController bulkController;

    private final AtomicBoolean closed = new AtomicBoolean(true);

    @Override
    public void init(Settings settings) throws IOException {
        if (closed.compareAndSet(true, false)) {
            super.init(settings);
            bulkController = new DefaultBulkController(this);
            logger.log(Level.INFO, "initializing bulk controller with settings = " + settings.toDelimitedString(','));
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
        Settings settings = indexDefinition.getSettings() == null ? null :
                Settings.builder().loadFromSource(indexDefinition.getSettings()).build();
        if (settings != null) {
            createIndexRequestBuilder.setSettings(settings);
        }
        // must be Map<String, Object> to match prototype of addMapping()!
        Map<String, Object> mappings = indexDefinition.getMappings() == null ? null :
                JsonXContent.jsonXContent.createParser(indexDefinition.getMappings()).mapOrdered();
        if (mappings != null) {
            logger.info("mappings = " + mappings);
            createIndexRequestBuilder.addMapping(type, mappings);
        } else {
            createIndexRequestBuilder.addMapping(type,
                    JsonXContent.contentBuilder().startObject().startObject(type).endObject().endObject());
        }
        CreateIndexResponse createIndexResponse = createIndexRequestBuilder.execute().actionGet();
        if (createIndexResponse.isAcknowledged()) {
            logger.info("index {} created", index);
        } else {
            logger.warn("index creation of {} not acknowledged", index);
        }
        refreshIndex(index);
    }

    @Override
    public void startBulk(IndexDefinition indexDefinition) throws IOException {
        startBulk(indexDefinition.getFullIndexName(), -1, 1);
    }

    @Override
    public void startBulk(String index, long startRefreshIntervalSeconds, long stopRefreshIntervalSeconds)
            throws IOException {
        ensureClientIsPresent();
        bulkController.startBulkMode(index, startRefreshIntervalSeconds, stopRefreshIntervalSeconds);
    }

    @Override
    public void stopBulk(IndexDefinition indexDefinition) throws IOException {
        ensureClientIsPresent();
        bulkController.stopBulkMode(indexDefinition);
    }

    @Override
    public void stopBulk(String index, long timeout, TimeUnit timeUnit) throws IOException {
        ensureClientIsPresent();
        bulkController.stopBulkMode(index, timeout, timeUnit);
    }

    @Override
    public BulkClient index(String index, String type, String id, boolean create, String source) {
        return index(new IndexRequest()
                .index(index)
                .type(type)
                .id(id)
                .create(create)
                .source(source)); // will be converted into a bytes reference
    }

    @Override
    public BulkClient index(String index, String type, String id, boolean create, BytesReference source) {
        return index(new IndexRequest()
                .index(index)
                .type(type)
                .id(id)
                .create(create)
                .source(source));
    }

    @Override
    public BulkClient index(IndexRequest indexRequest) {
        ensureClientIsPresent();
        bulkController.bulkIndex(indexRequest);
        return this;
    }

    @Override
    public BulkClient delete(String index, String type, String id) {
        return delete(new DeleteRequest()
                .index(index)
                .type(type)
                .id(id));
    }

    @Override
    public BulkClient delete(DeleteRequest deleteRequest) {
        ensureClientIsPresent();
        bulkController.bulkDelete(deleteRequest);
        return this;
    }

    @Override
    public BulkClient update(String index, String type, String id, String source) {
        return update(index, type, id, new BytesArray(source.getBytes(StandardCharsets.UTF_8)));
    }

    @Override
    public BulkClient update(String index, String type, String id, BytesReference source) {
        return update(new UpdateRequest()
                .index(index)
                .type(type)
                .id(id)
                .doc(source.hasArray() ? source.array() : source.toBytes()));
    }

    @Override
    public BulkClient update(UpdateRequest updateRequest) {
        ensureClientIsPresent();
        bulkController.bulkUpdate(updateRequest);
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
    public void flushIndex(String index) {
        if (index != null) {
            ensureClientIsPresent();
            client.execute(FlushAction.INSTANCE, new FlushRequest(index)).actionGet();
        }
    }

    @Override
    public void refreshIndex(String index) {
        if (index != null) {
            ensureClientIsPresent();
            client.execute(RefreshAction.INSTANCE, new RefreshRequest(index)).actionGet();
        }
    }
}
