package org.xbib.elasticsearch.extras.client.node;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.create.CreateIndexAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexAction;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingAction;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequestBuilder;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeValidationException;
import org.elasticsearch.plugins.Plugin;
import org.xbib.elasticsearch.extras.client.AbstractClient;
import org.xbib.elasticsearch.extras.client.BulkControl;
import org.xbib.elasticsearch.extras.client.BulkMetric;
import org.xbib.elasticsearch.extras.client.ClientMethods;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class BulkNodeClient extends AbstractClient implements ClientMethods {

    private static final Logger logger = LogManager.getLogger(BulkNodeClient.class.getName());

    private int maxActionsPerRequest = DEFAULT_MAX_ACTIONS_PER_REQUEST;

    private int maxConcurrentRequests = DEFAULT_MAX_CONCURRENT_REQUESTS;

    private ByteSizeValue maxVolume = DEFAULT_MAX_VOLUME_PER_REQUEST;

    private TimeValue flushInterval = DEFAULT_FLUSH_INTERVAL;

    private Node node;

    private ElasticsearchClient client;

    private BulkProcessor bulkProcessor;

    private BulkMetric metric;

    private BulkControl control;

    private Throwable throwable;

    private boolean closed;

    @Override
    public BulkNodeClient maxActionsPerRequest(int maxActionsPerRequest) {
        this.maxActionsPerRequest = maxActionsPerRequest;
        return this;
    }

    @Override
    public BulkNodeClient maxConcurrentRequests(int maxConcurrentRequests) {
        this.maxConcurrentRequests = maxConcurrentRequests;
        return this;
    }

    @Override
    public BulkNodeClient maxVolumePerRequest(ByteSizeValue maxVolume) {
        this.maxVolume = maxVolume;
        return this;
    }

    @Override
    public BulkNodeClient flushIngestInterval(TimeValue flushInterval) {
        this.flushInterval = flushInterval;
        return this;
    }

    @Override
    public BulkNodeClient init(ElasticsearchClient client,
                               final BulkMetric metric, final BulkControl control) {
        this.client = client;
        this.metric = metric;
        this.control = control;
        if (metric != null) {
            metric.start();
        }
        BulkProcessor.Listener listener = new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
                long l = -1;
                if (metric != null) {
                    metric.getCurrentIngest().inc();
                    l = metric.getCurrentIngest().getCount();
                    int n = request.numberOfActions();
                    metric.getSubmitted().inc(n);
                    metric.getCurrentIngestNumDocs().inc(n);
                    metric.getTotalIngestSizeInBytes().inc(request.estimatedSizeInBytes());
                }
                logger.debug("before bulk [{}] [actions={}] [bytes={}] [concurrent requests={}]",
                        executionId,
                        request.numberOfActions(),
                        request.estimatedSizeInBytes(),
                        l);
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                long l = -1;
                if (metric != null) {
                    metric.getCurrentIngest().dec();
                    l = metric.getCurrentIngest().getCount();
                    metric.getSucceeded().inc(response.getItems().length);
                }
                int n = 0;
                for (BulkItemResponse itemResponse : response.getItems()) {
                    if (metric != null) {
                        metric.getCurrentIngest().dec(itemResponse.getIndex(), itemResponse.getType(), itemResponse.getId());
                    }
                    if (itemResponse.isFailed()) {
                        n++;
                        if (metric != null) {
                            metric.getSucceeded().dec(1);
                            metric.getFailed().inc(1);
                        }
                    }
                }
                if (metric != null) {
                    logger.debug("after bulk [{}] [succeeded={}] [failed={}] [{}ms] {} concurrent requests",
                            executionId,
                            metric.getSucceeded().getCount(),
                            metric.getFailed().getCount(),
                            response.getTook().millis(),
                            l);
                }
                if (n > 0) {
                    logger.error("bulk [{}] failed with {} failed items, failure message = {}",
                            executionId, n, response.buildFailureMessage());
                } else {
                    if (metric != null) {
                        metric.getCurrentIngestNumDocs().dec(response.getItems().length);
                    }
                }
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                if (metric != null) {
                    metric.getCurrentIngest().dec();
                }
                throwable = failure;
                closed = true;
                logger.error("after bulk [" + executionId + "] error", failure);
            }
        };
        BulkProcessor.Builder builder = BulkProcessor.builder((Client) client, listener)
                .setBulkActions(maxActionsPerRequest)
                .setConcurrentRequests(maxConcurrentRequests)
                .setFlushInterval(flushInterval);
        if (maxVolume != null) {
            builder.setBulkSize(maxVolume);
        }
        this.bulkProcessor = builder.build();
        this.closed = false;
        return this;
    }

    @Override
    public BulkNodeClient init(Settings settings, BulkMetric metric, BulkControl control) throws IOException {
        createClient(settings);
        this.metric = metric;
        this.control = control;
        return this;
    }

    @Override
    public ElasticsearchClient client() {
        return client;
    }

    @Override
    protected synchronized void createClient(Settings settings) throws IOException {
        if (client != null) {
            logger.warn("client is open, closing...");
            client.threadPool().shutdown();
            client = null;
            node.close();
        }
        if (settings != null) {
            String version = System.getProperty("os.name")
                    + " " + System.getProperty("java.vm.name")
                    + " " + System.getProperty("java.vm.vendor")
                    + " " + System.getProperty("java.runtime.version")
                    + " " + System.getProperty("java.vm.version");
            Settings effectiveSettings = Settings.builder().put(settings)
                    .put("node.client", true)
                    .put("node.master", false)
                    .put("node.data", false).build();
            logger.info("creating node client on {} with effective settings {}",
                    version, effectiveSettings.getAsMap());
            Collection<Class<? extends Plugin>> plugins = Collections.emptyList();
            this.node = new BulkNode(new Environment(effectiveSettings), plugins);
            try {
                node.start();
            } catch (NodeValidationException e) {
                throw new IOException(e);
            }
            this.client = node.client();
        }
    }

    @Override
    public BulkMetric getMetric() {
        return metric;
    }

    @Override
    public BulkNodeClient index(String index, String type, String id, String source) {
        if (closed) {
            throwClose();
        }
        try {
            if (metric != null) {
                metric.getCurrentIngest().inc(index, type, id);
            }
            bulkProcessor.add(new IndexRequest(index).type(type).id(id).create(false).source(source));
        } catch (Exception e) {
            throwable = e;
            closed = true;
            logger.error("bulk add of index request failed: " + e.getMessage(), e);
        }
        return this;
    }

    @Override
    public BulkNodeClient bulkIndex(IndexRequest indexRequest) {
        if (closed) {
            throwClose();
        }
        try {
            if (metric != null) {
                metric.getCurrentIngest().inc(indexRequest.index(), indexRequest.type(), indexRequest.id());
            }
            bulkProcessor.add(indexRequest);
        } catch (Exception e) {
            throwable = e;
            closed = true;
            logger.error("bulk add of index request failed: " + e.getMessage(), e);
        }
        return this;
    }

    @Override
    public BulkNodeClient delete(String index, String type, String id) {
        if (closed) {
            throwClose();
        }
        try {
            if (metric != null) {
                metric.getCurrentIngest().inc(index, type, id);
            }
            bulkProcessor.add(new DeleteRequest(index).type(type).id(id));
        } catch (Exception e) {
            throwable = e;
            closed = true;
            logger.error("bulk add of delete failed: " + e.getMessage(), e);
        }
        return this;
    }

    @Override
    public BulkNodeClient bulkDelete(DeleteRequest deleteRequest) {
        if (closed) {
            throwClose();
        }
        try {
            if (metric != null) {
                metric.getCurrentIngest().inc(deleteRequest.index(), deleteRequest.type(), deleteRequest.id());
            }
            bulkProcessor.add(deleteRequest);
        } catch (Exception e) {
            throwable = e;
            closed = true;
            logger.error("bulk add of delete failed: " + e.getMessage(), e);
        }
        return this;
    }

    @Override
    public BulkNodeClient update(String index, String type, String id, String source) {
        if (closed) {
            throwClose();
        }
        try {
            if (metric != null) {
                metric.getCurrentIngest().inc(index, type, id);
            }
            bulkProcessor.add(new UpdateRequest().index(index).type(type).id(id).upsert(source));
        } catch (Exception e) {
            throwable = e;
            closed = true;
            logger.error("bulk add of update request failed: " + e.getMessage(), e);
        }
        return this;
    }

    @Override
    public BulkNodeClient bulkUpdate(UpdateRequest updateRequest) {
        if (closed) {
            throwClose();
        }
        try {
            if (metric != null) {
                metric.getCurrentIngest().inc(updateRequest.index(), updateRequest.type(), updateRequest.id());
            }
            bulkProcessor.add(updateRequest);
        } catch (Exception e) {
            throwable = e;
            closed = true;
            logger.error("bulk add of update request failed: " + e.getMessage(), e);
        }
        return this;
    }

    @Override
    public BulkNodeClient flushIngest() {
        if (closed) {
            throwClose();
        }
        logger.debug("flushing bulk processor");
        bulkProcessor.flush();
        return this;
    }

    @Override
    public BulkNodeClient waitForResponses(TimeValue maxWaitTime) throws InterruptedException, ExecutionException {
        if (closed) {
            throwClose();
        }
        while (!bulkProcessor.awaitClose(maxWaitTime.getMillis(), TimeUnit.MILLISECONDS)) {
            logger.warn("still waiting for responses");
        }
        return this;
    }

    @Override
    public BulkNodeClient startBulk(String index, long startRefreshIntervalMillis, long stopRefreshItervalMillis)
            throws IOException {
        if (control == null) {
            return this;
        }
        if (!control.isBulk(index)) {
            control.startBulk(index, startRefreshIntervalMillis, stopRefreshItervalMillis);
            updateIndexSetting(index, "refresh_interval", startRefreshIntervalMillis + "ms");
        }
        return this;
    }

    @Override
    public BulkNodeClient stopBulk(String index) throws IOException {
        if (control == null) {
            return this;
        }
        if (control.isBulk(index)) {
            updateIndexSetting(index, "refresh_interval", control.getStopBulkRefreshIntervals().get(index) + "ms");
            control.finishBulk(index);
        }
        return this;
    }

    @Override
    public synchronized void shutdown() {
        try {
            if (bulkProcessor != null) {
                logger.debug("closing bulk processor...");
                bulkProcessor.close();
            }
            if (control != null && control.indices() != null && !control.indices().isEmpty()) {
                logger.debug("stopping bulk mode for indices {}...", control.indices());
                for (String index : control.indices()) {
                    stopBulk(index);
                }
                metric.stop();
            }
            if (node != null) {
                logger.debug("closing node...");
                node.close();
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    @Override
    public BulkNodeClient newIndex(String index) {
        return newIndex(index, null, null);
    }

    @Override
    public BulkNodeClient newIndex(String index, String type, InputStream settings, InputStream mappings) throws IOException {
        resetSettings();
        setting(settings);
        mapping(type, mappings);
        return newIndex(index, settings(), mappings());
    }

    @Override
    public BulkNodeClient newIndex(String index, Settings settings, Map<String, String> mappings) {
        if (closed) {
            throwClose();
        }
        if (client == null) {
            logger.warn("no client for create index");
            return this;
        }
        if (index == null) {
            logger.warn("no index name given to create index");
            return this;
        }
        CreateIndexRequestBuilder createIndexRequestBuilder =
                new CreateIndexRequestBuilder(client(), CreateIndexAction.INSTANCE).setIndex(index);
        if (settings != null) {
            logger.info("settings = {}", settings.getAsStructuredMap());
            createIndexRequestBuilder.setSettings(settings);
        }
        if (mappings != null) {
            for (Map.Entry<String, String> entry : mappings.entrySet()) {
                String type = entry.getKey();
                String mapping = entry.getValue();
                logger.info("found mapping for {}", type);
                createIndexRequestBuilder.addMapping(type, mapping);
            }
        }
        createIndexRequestBuilder.execute().actionGet();
        logger.info("index {} created", index);
        return this;
    }

    @Override
    public BulkNodeClient newMapping(String index, String type, Map<String, Object> mapping) {
        PutMappingRequestBuilder putMappingRequestBuilder =
                new PutMappingRequestBuilder(client(), PutMappingAction.INSTANCE)
                        .setIndices(index)
                        .setType(type)
                        .setSource(mapping);
        putMappingRequestBuilder.execute().actionGet();
        logger.info("mapping created for index {} and type {}", index, type);
        return this;
    }

    @Override
    public BulkNodeClient deleteIndex(String index) {
        if (closed) {
            throwClose();
        }
        if (client == null) {
            logger.warn("no client");
            return this;
        }
        if (index == null) {
            logger.warn("no index name given to delete index");
            return this;
        }
        DeleteIndexRequestBuilder deleteIndexRequestBuilder =
                new DeleteIndexRequestBuilder(client(), DeleteIndexAction.INSTANCE, index);
        deleteIndexRequestBuilder.execute().actionGet();
        return this;
    }

    @Override
    public boolean hasThrowable() {
        return throwable != null;
    }

    @Override
    public Throwable getThrowable() {
        return throwable;
    }

    public Settings getSettings() {
        return settings();
    }

    @Override
    public Settings.Builder getSettingsBuilder() {
        return settingsBuilder();
    }

    private static void throwClose() {
        throw new ElasticsearchException("client is closed");
    }

    private class BulkNode extends Node {

        BulkNode(Environment env, Collection<Class<? extends Plugin>> classpathPlugins) {
            super(env, classpathPlugins);
        }
    }

}
