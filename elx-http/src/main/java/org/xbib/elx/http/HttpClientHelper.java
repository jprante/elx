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
import org.xbib.elx.common.Parameters;
import org.xbib.net.SocketConfig;
import org.xbib.net.URL;
import org.xbib.net.http.HttpAddress;
import org.xbib.net.http.client.netty.NettyHttpClient;
import org.xbib.net.http.client.netty.NettyHttpClientBuilder;
import org.xbib.net.http.client.netty.NettyHttpClientConfig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Elasticsearch HTTP client.
 */
public class HttpClientHelper {

    private static final Logger logger = LogManager.getLogger(HttpClientHelper.class);

    private NettyHttpClient nettyHttpClient;

    private final ClassLoader classLoader;

    private final NamedXContentRegistry registry;

    @SuppressWarnings("rawtypes")
    private final Map<ActionType, HttpAction> actionMap;

    private String url;

    private final AtomicBoolean closed;

    public HttpClientHelper(ClassLoader classLoader) {
        this(Collections.emptyList(), classLoader);
    }

    public HttpClientHelper(List<NamedXContentRegistry.Entry> namedXContentEntries,
                            ClassLoader classLoader) {
        this.registry = new NamedXContentRegistry(Stream.of(getNamedXContents().stream(),
                namedXContentEntries.stream()).flatMap(Function.identity()).collect(Collectors.toList()));
        this.classLoader = classLoader;
        this.actionMap = new HashMap<>();
        this.closed = new AtomicBoolean();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public void init(Settings settings) throws IOException {
        HttpAddress httpAddress;
        if (settings.hasValue("url")) {
            this.url = settings.get("url");
            httpAddress = HttpAddress.http1(this.url);
        } else if (settings.hasValue(Parameters.HOST.getName())) {
            // use only first host
            URL u = findAddresses(settings).stream().findFirst()
                    .orElseGet(() -> URL.http().host("localhost").port(9200).build());
            httpAddress = HttpAddress.http1(u);
            this.url = u.toExternalForm();
        } else {
            URL u = URL.http().host("localhost").port(9200).build();
            httpAddress = HttpAddress.http1(u);
            this.url = u.toExternalForm();
        }
        ServiceLoader<HttpAction> httpActionServiceLoader = ServiceLoader.load(HttpAction.class, classLoader);
        for (HttpAction<? extends ActionRequest, ? extends ActionResponse> httpAction : httpActionServiceLoader) {
            httpAction.setSettings(settings);
            actionMap.put(httpAction.getActionInstance(), httpAction);
        }
        NettyHttpClientConfig clientConfig = new NettyHttpClientConfig();
        if (settings.hasValue("debug")) {
            clientConfig.enableDebug();
        }
        SocketConfig socketConfig = new SocketConfig();
        socketConfig.setConnectTimeoutMillis(settings.getAsInt("http.connect_timeout", 5000));
        socketConfig.setReadTimeoutMillis(settings.getAsInt("http.read_timeout", 30000));
        clientConfig.setSocketConfig(socketConfig);
        NettyHttpClientBuilder clientBuilder = NettyHttpClient.builder()
                .setConfig(clientConfig);
        this.nettyHttpClient =  clientBuilder.build();
        if (logger.isDebugEnabled()) {
            logger.log(Level.DEBUG, "HTTP client initialized, settings = {}, url = {}, {} actions",
                    settings, url, actionMap.size());
        }
    }

    private static List<NamedXContentRegistry.Entry> getNamedXContents() {
        return new ArrayList<>();
    }

    public NamedXContentRegistry getRegistry() {
        return registry;
    }

    public NettyHttpClient internalClient() {
        return nettyHttpClient;
    }

    protected void closeClient(Settings settings) {
        if (closed.compareAndSet(false, true)) {
            try {
                nettyHttpClient.close();
            } catch (IOException e) {
                logger.log(Level.WARN, e.getMessage(), e);
            }
        }
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
            if (logger.isTraceEnabled()) {
                logger.log(Level.TRACE, "url = " + url);
            }
            httpAction.execute(httpActionContext, listener);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    private List<URL> findAddresses(Settings settings) {
        final int defaultPort = settings.getAsInt(Parameters.PORT.getName(), 9200);
        List<URL> addresses = new ArrayList<>();
        for (String hostname : settings.getAsList(Parameters.HOST.getName())) {
            String[] splitHost = hostname.split(":", 2);
            if (splitHost.length == 2) {
                try {
                    String host = splitHost[0];
                    int port = Integer.parseInt(splitHost[1]);
                    addresses.add(URL.from("http://" + host + ":" + port));
                } catch (NumberFormatException e) {
                    logger.warn(e.getMessage(), e);
                }
            }
            if (splitHost.length == 1) {
                String host = splitHost[0];
                addresses.add(URL.from("http://" + host + ":" + defaultPort));
            }
        }
        return addresses;
    }
}
