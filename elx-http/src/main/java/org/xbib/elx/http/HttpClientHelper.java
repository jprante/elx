package org.xbib.elx.http;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.threadpool.ThreadPool;
import org.xbib.net.URL;
import org.xbib.netty.http.client.Client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Elasticsearch HTTP client.
 */
public class HttpClientHelper {

    private static final Logger logger = LogManager.getLogger(HttpClientHelper.class);

    private Client nettyHttpClient;

    private final ClassLoader classLoader;

    private final NamedXContentRegistry registry;

    @SuppressWarnings("rawtypes")
    private final Map<ActionType, HttpAction> actionMap;

    private String url;

    public HttpClientHelper() {
        this(Collections.emptyList(), Thread.currentThread().getContextClassLoader());
    }

    public HttpClientHelper(List<NamedXContentRegistry.Entry> namedXContentEntries,
                            ClassLoader classLoader) {
        this.registry = new NamedXContentRegistry(Stream.of(getNamedXContents().stream(),
                namedXContentEntries.stream()).flatMap(Function.identity()).collect(Collectors.toList()));
        this.classLoader = classLoader != null ? classLoader : Thread.currentThread().getContextClassLoader();
        this.actionMap = new HashMap<>();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public HttpClientHelper init(Settings settings) throws IOException {
        if (settings.hasValue("url")) {
            this.url = settings.get("url");
        } else if (settings.hasValue("host")) {
            this.url = URL.http()
                    .host(settings.get("host")).port(settings.getAsInt("port", 9200))
                    .build()
                    .toExternalForm();
        }
        ServiceLoader<HttpAction> httpActionServiceLoader = ServiceLoader.load(HttpAction.class, classLoader);
        for (HttpAction<? extends ActionRequest, ? extends ActionResponse> httpAction : httpActionServiceLoader) {
            httpAction.setSettings(settings);
            actionMap.put(httpAction.getActionInstance(), httpAction);
        }
        Client.Builder clientBuilder = Client.builder();
        if (settings.hasValue("debug")) {
            clientBuilder.enableDebug();
        }
        this.nettyHttpClient = clientBuilder.build();
        logger.log(Level.DEBUG, "extended HTTP client initialized, settings = {}, url = {}, {} actions",
                settings, url, actionMap.size());
        return this;
    }

    private static List<NamedXContentRegistry.Entry> getNamedXContents() {
        return new ArrayList<>();
    }

    public NamedXContentRegistry getRegistry() {
        return registry;
    }

    public Client internalClient() {
        return nettyHttpClient;
    }

    protected void closeClient(Settings settings) throws IOException {
        nettyHttpClient.shutdownGracefully();
    }

    public <Request extends ActionRequest, Response extends ActionResponse>
    ActionFuture<Response> execute(ActionType<Response> action, Request request) {
        PlainActionFuture<Response> actionFuture = PlainActionFuture.newFuture();
        execute(action, request, actionFuture);
        return actionFuture;
    }

    public <Request extends ActionRequest, Response extends ActionResponse>
    void execute(ActionType<Response> action, Request request, ActionListener<Response> listener) {
        doExecute(action, request, listener);
    }

    public ThreadPool threadPool() {
        logger.log(Level.DEBUG, "returning null for threadPool() request");
        return null;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private <R extends ActionRequest, T extends ActionResponse, B extends ActionRequestBuilder<R, T>>
    void doExecute(ActionType<T> action, R request, ActionListener<T> listener) {
        HttpAction httpAction = actionMap.get(action);
        if (httpAction == null) {
            throw new IllegalStateException("failed to find http action [" + action + "] to execute");
        }
        try {
            HttpActionContext httpActionContext = new HttpActionContext(this, request, url);
            logger.log(Level.DEBUG, "url = " + url);
            httpAction.execute(httpActionContext, listener);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }
}