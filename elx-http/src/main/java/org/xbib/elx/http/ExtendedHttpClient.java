package org.xbib.elx.http;

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
import org.elasticsearch.threadpool.ThreadPool;
import org.xbib.elx.common.AbstractExtendedClient;
import org.xbib.netty.http.client.Client;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

/**
 * Elasticsearch HTTP client.
 */
public class ExtendedHttpClient extends AbstractExtendedClient implements ElasticsearchClient {

    private static final Logger logger = LogManager.getLogger(ExtendedHttpClient.class);

    private Client nettyHttpClient;

    private final ClassLoader classLoader;

    @SuppressWarnings("rawtypes")
    private final Map<GenericAction, HttpAction> actionMap;

    private String url;

    public ExtendedHttpClient() {
        this.classLoader = ExtendedHttpClient.class.getClassLoader();
        this.actionMap = new HashMap<>();
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public ExtendedHttpClient init(Settings settings) throws IOException {
        super.init(settings);
        if (settings == null) {
            return null;
        }
        this.url = settings.get("url");
        ServiceLoader<HttpAction> httpActionServiceLoader = ServiceLoader.load(HttpAction.class, classLoader);
        for (HttpAction<? extends ActionRequest, ? extends ActionResponse> httpAction : httpActionServiceLoader) {
            httpAction.setSettings(settings);
            actionMap.put(httpAction.getActionInstance(), httpAction);
        }
        this.nettyHttpClient = Client.builder().enableDebug().build();
        logger.info("extended HTTP client initialized with {} actions", actionMap.size());
        return this;
    }

    public Client internalClient() {
        return nettyHttpClient;
    }

    @Override
    public ElasticsearchClient getClient() {
        return this;
    }

    @Override
    protected ElasticsearchClient createClient(Settings settings) {
        return this;
    }

    @Override
    protected void closeClient() throws IOException {
        nettyHttpClient.shutdownGracefully();
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse,
            RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>> ActionFuture<Response>
    execute(Action<Request, Response, RequestBuilder> action, Request request) {
        PlainActionFuture<Response> actionFuture = PlainActionFuture.newFuture();
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
        throw new UnsupportedOperationException();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private <R extends ActionRequest, T extends ActionResponse, B extends ActionRequestBuilder<R, T, B>>
    void doExecute(Action<R, T, B> action, R request, ActionListener<T> listener) {
        HttpAction httpAction = actionMap.get(action);
        if (httpAction == null) {
            throw new IllegalStateException("failed to find http action [" + action + "] to execute");
        }
        try {
            HttpActionContext httpActionContext = new HttpActionContext(this, request, url);
            if (logger.isDebugEnabled()) {
                logger.debug("submitting request {} to URL {}", request, url);
            }
            httpAction.execute(httpActionContext, listener);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }
}
