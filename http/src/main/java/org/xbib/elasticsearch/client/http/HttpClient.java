package org.xbib.elasticsearch.client.http;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.GenericAction;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.node.Node;
import org.elasticsearch.threadpool.ThreadPool;
import org.xbib.elasticsearch.client.AbstractClient;
import org.xbib.elasticsearch.client.BulkControl;
import org.xbib.elasticsearch.client.BulkMetric;
import org.xbib.netty.http.client.Client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Elasticsearch HTTP client.
 */
public class HttpClient extends AbstractClient implements ElasticsearchClient {

    private static final Logger logger = LogManager.getLogger(HttpClient.class);

    private Client client;

    private NamedXContentRegistry registry;

    @SuppressWarnings("rawtypes")
    private Map<GenericAction, HttpAction> actionMap;

    private List<String> urls;

    //private ThreadPool threadPool;

    @Override
    public HttpClient init(ElasticsearchClient client, Settings settings, BulkMetric metric, BulkControl control) {
        init(client, settings, metric, control, null, Collections.emptyList());
        return this;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void init(ElasticsearchClient client, Settings settings, BulkMetric metric, BulkControl control,
                            ClassLoader classLoader, List<NamedXContentRegistry.Entry> namedXContentEntries) {
        //super.init(client, settings, metric, control);
        this.urls = settings.getAsList("urls");
        if (urls.isEmpty()) {
            throw new IllegalArgumentException("no urls given");
        }
        this.registry = new NamedXContentRegistry(Stream.of(getNamedXContents().stream(),
                namedXContentEntries.stream()
        ).flatMap(Function.identity()).collect(Collectors.toList()));
        this.actionMap = new HashMap<>();
        ServiceLoader<HttpAction> httpActionServiceLoader = ServiceLoader.load(HttpAction.class,
                classLoader != null ? classLoader : Thread.currentThread().getContextClassLoader());
        for (HttpAction<? extends ActionRequest, ? extends ActionResponse> httpAction : httpActionServiceLoader) {
            httpAction.setSettings(settings);
            actionMap.put(httpAction.getActionInstance(), httpAction);
        }
        this.client = Client.builder().enableDebug().build();
        Settings threadPoolsettings = Settings.builder()
                .put(settings)
                .put(Node.NODE_NAME_SETTING.getKey(), "httpclient")
                .build();
        //this.threadPool = threadPool != null ? threadPool : new ThreadPool(threadPoolsettings);
        logger.info("HTTP client initialized with {} actions", actionMap.size());
    }

    private static List<NamedXContentRegistry.Entry> getNamedXContents() {
        return new ArrayList<>();
    }

    public NamedXContentRegistry getRegistry() {
        return registry;
    }

    public static Builder builder() {
        return new Builder();
    }

    public Client internalClient() {
        return client;
    }

    @Override
    public ElasticsearchClient client() {
        return this;
    }

    @Override
    protected ElasticsearchClient createClient(Settings settings) throws IOException {
        return this;
    }

    @Override
    public void shutdown() throws IOException {
        client.shutdownGracefully();
        //threadPool.close();
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse,
            RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>> ActionFuture<Response>
    execute(Action<Request, Response, RequestBuilder> action, Request request) {
        PlainActionFuture<Response> actionFuture = PlainActionFuture.newFuture();
        logger.info("plain action future = " + actionFuture);
        execute(action, request, actionFuture);
        return actionFuture;
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse,
            RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>> void
    execute(Action<Request, Response, RequestBuilder> action, Request request, ActionListener<Response> listener) {
        doExecute(action, request, listener);
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse,
            RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>> RequestBuilder
    prepareExecute(Action<Request, Response, RequestBuilder> action) {
        return action.newRequestBuilder(this);
    }

    @Override
    public ThreadPool threadPool() {
        logger.info("returning null for threadPool() request");
        return null; //threadPool;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public <R extends ActionRequest, T extends ActionResponse, B extends ActionRequestBuilder<R, T, B>>
    void doExecute(Action<R, T, B> action, R request, ActionListener<T> listener) {
        HttpAction httpAction = actionMap.get(action);
        if (httpAction == null) {
            throw new IllegalStateException("failed to find http action [" + action + "] to execute");
        }
        logger.info("http action = " + httpAction);
        String url = urls.get(0); // TODO
        try {
            logger.info("submitting to URL {}", url);
            HttpActionContext httpActionContext = new HttpActionContext(this, request, url);
            httpAction.execute(httpActionContext, listener);
            logger.info("submitted to URL {}", url);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    /**
     * The Builder for HTTP client.
     */
    public static class Builder {

        private final Settings.Builder settingsBuilder = Settings.builder();

        private ClassLoader classLoader;

        private List<NamedXContentRegistry.Entry> namedXContentEntries;

        private ThreadPool threadPool = null;

        public Builder settings(Settings settings) {
            this.settingsBuilder.put(settings);
            return this;
        }

        public Builder classLoader(ClassLoader classLoader) {
            this.classLoader = classLoader;
            return this;
        }

        public Builder namedXContentEntries(List<NamedXContentRegistry.Entry> namedXContentEntries) {
            this.namedXContentEntries = namedXContentEntries;
            return this;
        }

        public Builder threadPool(ThreadPool threadPool) {
            this.threadPool = threadPool;
            return this;
        }

        @SuppressWarnings({"unchecked", "rawtypes"})
        public HttpClient build() {
            Settings settings = settingsBuilder.build();
            HttpClient httpClient = new HttpClient();
            httpClient.init(null, settings, null, null,
                    classLoader, namedXContentEntries);
            return httpClient;
        }
    }
}
