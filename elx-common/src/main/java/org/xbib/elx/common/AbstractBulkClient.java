package org.xbib.elx.common;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.indices.create.CreateIndexAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
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

public abstract class AbstractBulkClient extends AbstractBasicClient implements BulkClient {

    private static final Logger logger = LogManager.getLogger(AbstractBulkClient.class.getName());

    private BulkController bulkController;

    private final AtomicBoolean closed = new AtomicBoolean(true);

    @Override
    public void init(Settings settings) throws IOException {
        if (closed.compareAndSet(true, false)) {
            super.init(settings);
            logger.log(Level.INFO, "initializing with settings = " + settings.toDelimitedString(','));
            bulkController = new DefaultBulkController(this);
            bulkController.init(settings);
        } else {
            logger.log(Level.WARN, "not initializing");
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
        Settings settings = indexDefinition.getSettings() == null ? null :
                Settings.builder().loadFromSource(indexDefinition.getSettings(), XContentType.JSON).build();
        Map<String, ?> mappings = indexDefinition.getMappings() == null ? null :
                JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY,
                        DeprecationHandler.THROW_UNSUPPORTED_OPERATION, indexDefinition.getMappings()).mapOrdered();
        newIndex(indexDefinition.getFullIndexName(), settings, mappings);
    }

    @Override
    public void newIndex(String index) throws IOException {
        newIndex(index, Settings.EMPTY, (Map<String, ?>) null);
    }

    @Override
    public void newIndex(String index, Settings settings) throws IOException {
        newIndex(index, settings, (Map<String, ?>) null);
    }

    @Override
    public void newIndex(String index, Settings settings, XContentBuilder builder) throws IOException {
        String mappingString = Strings.toString(builder);
        Map<String, ?> mappings = JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION, mappingString).mapOrdered();
        newIndex(index, settings, mappings);
    }

    @Override
    public void newIndex(String index, Settings settings, Map<String, ?> mapping) throws IOException {
        if (index == null) {
            logger.warn("no index name given to create index");
            return;
        }
        ensureClientIsPresent();
        waitForCluster("YELLOW", 30L, TimeUnit.SECONDS);
        CreateIndexRequest createIndexRequest = new CreateIndexRequest().index(index);
        if (settings != null) {
            createIndexRequest.settings(settings);
        }
        if (mapping != null) {
            createIndexRequest.mapping("_doc", mapping);
        }
        CreateIndexResponse createIndexResponse = client.execute(CreateIndexAction.INSTANCE, createIndexRequest).actionGet();
        if (createIndexResponse.isAcknowledged()) {
            logger.info("index {} created", index);
        }
    }

    @Override
    public void startBulk(IndexDefinition indexDefinition) throws IOException {
        startBulk(indexDefinition.getFullIndexName(), -1, 1);
    }

    @Override
    public void startBulk(String index, long startRefreshIntervalSeconds, long stopRefreshIntervalSeconds)
            throws IOException {
        if (bulkController != null) {
            ensureClientIsPresent();
            bulkController.startBulkMode(index, startRefreshIntervalSeconds, stopRefreshIntervalSeconds);
        }
    }

    @Override
    public void stopBulk(IndexDefinition indexDefinition) throws IOException {
        if (bulkController != null) {
            ensureClientIsPresent();
            bulkController.stopBulkMode(indexDefinition);
        }
    }

    @Override
    public void stopBulk(String index, long timeout, TimeUnit timeUnit) throws IOException {
        if (bulkController != null) {
            ensureClientIsPresent();
            bulkController.stopBulkMode(index, timeout, timeUnit);
        }
    }

    @Override
    public BulkClient index(String index, String id, boolean create, String source) {
        return index(index, id, create, new BytesArray(source.getBytes(StandardCharsets.UTF_8)));
    }

    @Override
    public BulkClient index(String index, String id, boolean create, BytesReference source) {
        return index(new IndexRequest().index(index).id(id).create(create)
                .source(source, XContentType.JSON));
    }

    @Override
    public BulkClient index(IndexRequest indexRequest) {
        ensureClientIsPresent();
        bulkController.bulkIndex(indexRequest);
        return this;
    }

    @Override
    public BulkClient delete(String index, String id) {
        return delete(new DeleteRequest().index(index).id(id));
    }

    @Override
    public BulkClient delete(DeleteRequest deleteRequest) {
        ensureClientIsPresent();
        bulkController.bulkDelete(deleteRequest);
        return this;
    }

    @Override
    public BulkClient update(String index, String id, BytesReference source) {
        return update(new UpdateRequest().index(index).id(id)
                .doc(source, XContentType.JSON));
    }

    @Override
    public BulkClient update(String index, String id, String source) {
        return update(new UpdateRequest().index(index).id(id)
                .doc(source.getBytes(StandardCharsets.UTF_8), XContentType.JSON));
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
