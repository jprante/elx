package org.xbib.elasticsearch.client.transport;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.node.liveness.TransportLivenessAction;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.metrics.MeanMetric;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.ConcurrentMapLong;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ActionNotFoundTransportException;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.FutureTransportResponseHandler;
import org.elasticsearch.transport.NodeDisconnectedException;
import org.elasticsearch.transport.NodeNotConnectedException;
import org.elasticsearch.transport.PlainTransportFuture;
import org.elasticsearch.transport.ReceiveTimeoutTransportException;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.transport.RequestHandlerRegistry;
import org.elasticsearch.transport.ResponseHandlerFailureTransportException;
import org.elasticsearch.transport.SendRequestTransportException;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportFuture;
import org.elasticsearch.transport.TransportInterceptor;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportResponseOptions;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledFuture;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 *
 */
public class XbibTransportService extends AbstractLifecycleComponent {

    private static final String HANDSHAKE_ACTION_NAME = "internal:transport/handshake";

    private static final Setting<List<String>> TRACE_LOG_INCLUDE_SETTING =
            Setting.listSetting("transport.tracer.include", Collections.emptyList(), Function.identity(),
                    Property.Dynamic, Property.NodeScope);

    private static final Setting<List<String>> TRACE_LOG_EXCLUDE_SETTING =
            Setting.listSetting("transport.tracer.exclude", Arrays.asList("internal:discovery/zen/fd*",
                    TransportLivenessAction.NAME), Function.identity(), Property.Dynamic, Property.NodeScope);

    private final CountDownLatch blockIncomingRequestsLatch = new CountDownLatch(1);

    private final Transport transport;

    private final ThreadPool threadPool;

    private final ClusterName clusterName;

    private final TaskManager taskManager;

    private final TransportInterceptor.AsyncSender asyncSender;

    private final Function<BoundTransportAddress, DiscoveryNode> localNodeFactory;

    private volatile Map<String, RequestHandlerRegistry<TransportRequest>> requestHandlers = Collections.emptyMap();

    private final Object requestHandlerMutex = new Object();

    private final ConcurrentMapLong<RequestHolder<TransportResponse>> clientHandlers =
            ConcurrentCollections.newConcurrentMapLongWithAggressiveConcurrency();

    private final TransportInterceptor interceptor;

    // An LRU (don't really care about concurrency here) that holds the latest timed out requests so if they
    // do show up, we can print more descriptive information about them
    private final Map<Long, TimeoutInfoHolder> timeoutInfoHandlers =
        Collections.synchronizedMap(new LinkedHashMap<Long, TimeoutInfoHolder>(100, .75F, true) {
            private static final long serialVersionUID = 9174428975922394994L;

            @Override
            protected boolean removeEldestEntry(Map.Entry<Long, TimeoutInfoHolder> eldest) {
                return size() > 100;
            }
        });

    private final Logger tracerLog;

    private volatile String[] tracerLogInclude;

    private volatile String[] tracerLogExclude;

    private volatile DiscoveryNode localNode = null;

    private final Transport.Connection localNodeConnection = new Transport.Connection() {
        @Override
        public DiscoveryNode getNode() {
            return localNode;
        }

        @Override
        public void sendRequest(long requestId, String action, TransportRequest request, TransportRequestOptions options)
            throws IOException, TransportException {
            sendLocalRequest(requestId, action, request, options);
        }

        @Override
        public void close() throws IOException {
        }
    };

    /**
     * Build the service.
     *
     * @param clusterSettings if non null the the {@linkplain XbibTransportService} will register
     *                        with the {@link ClusterSettings} for settings updates for
     *                        {@link #TRACE_LOG_EXCLUDE_SETTING} and {@link #TRACE_LOG_INCLUDE_SETTING}.
     */
    XbibTransportService(Settings settings, Transport transport, ThreadPool threadPool,
                         TransportInterceptor transportInterceptor,
                         Function<BoundTransportAddress, DiscoveryNode> localNodeFactory,
                         @Nullable ClusterSettings clusterSettings) {
        super(settings);
        this.transport = transport;
        this.threadPool = threadPool;
        this.localNodeFactory = localNodeFactory;
        this.clusterName = ClusterName.CLUSTER_NAME_SETTING.get(settings);
        setTracerLogInclude(TRACE_LOG_INCLUDE_SETTING.get(settings));
        setTracerLogExclude(TRACE_LOG_EXCLUDE_SETTING.get(settings));
        tracerLog = Loggers.getLogger(logger, ".tracer");
        taskManager = createTaskManager();
        this.interceptor = transportInterceptor;
        this.asyncSender = interceptor.interceptSender(this::sendRequestInternal);
        if (clusterSettings != null) {
            clusterSettings.addSettingsUpdateConsumer(TRACE_LOG_INCLUDE_SETTING, this::setTracerLogInclude);
            clusterSettings.addSettingsUpdateConsumer(TRACE_LOG_EXCLUDE_SETTING, this::setTracerLogExclude);
        }
    }

    private TaskManager createTaskManager() {
        return new TaskManager(settings);
    }

    private void setTracerLogInclude(List<String> tracerLogInclude) {
        this.tracerLogInclude = tracerLogInclude.toArray(Strings.EMPTY_ARRAY);
    }

    private void setTracerLogExclude(List<String> tracerLogExclude) {
        this.tracerLogExclude = tracerLogExclude.toArray(Strings.EMPTY_ARRAY);
    }

    @Override
    protected void doStart() {
        rxMetric.clear();
        txMetric.clear();
        transport.setTransportService(this);
        transport.start();
        if (transport.boundAddress() != null && logger.isInfoEnabled()) {
            logger.info("{}", transport.boundAddress());
            for (Map.Entry<String, BoundTransportAddress> entry : transport.profileBoundAddresses().entrySet()) {
                logger.info("profile [{}]: {}", entry.getKey(), entry.getValue());
            }
        }
        localNode = localNodeFactory.apply(transport.boundAddress());
        registerRequestHandler(HANDSHAKE_ACTION_NAME,
            () -> HandshakeRequest.INSTANCE,
            ThreadPool.Names.SAME,
            (request, channel) -> channel.sendResponse(new HandshakeResponse(localNode, clusterName,
                    localNode.getVersion())));
    }

    @Override
    protected void doStop() {
        try {
            transport.stop();
        } finally {
            // in case the transport is not connected to our local node (thus cleaned on node disconnect)
            // make sure to clean any leftover on going handles
            for (Map.Entry<Long, RequestHolder<TransportResponse>> entry : clientHandlers.entrySet()) {
                final RequestHolder<? extends TransportResponse> holderToNotify = clientHandlers.remove(entry.getKey());
                if (holderToNotify != null) {
                    // callback that an exception happened, but on a different thread since we don't
                    // want handlers to worry about stack overflows
                    threadPool.generic().execute(new AbstractRunnable() {
                        @Override
                        public void onRejection(Exception e) {
                            // if we get rejected during node shutdown we don't wanna bubble it up
                            logger.debug((Supplier<?>) () -> new ParameterizedMessage(
                                    "failed to notify response handler on rejection, action: {}",
                                    holderToNotify.action()),
                                e);
                        }
                        @Override
                        public void onFailure(Exception e) {
                            logger.warn((Supplier<?>) () -> new ParameterizedMessage(
                                    "failed to notify response handler on exception, action: {}",
                                    holderToNotify.action()),
                                e);
                        }
                        @Override
                        public void doRun() {
                            TransportException ex = new TransportException("transport stopped, action: " +
                                    holderToNotify.action());
                            holderToNotify.handler().handleException(ex);
                        }
                    });
                }
            }
        }
    }

    @Override
    protected void doClose() {
        transport.close();
    }

    /**
     * Start accepting incoming requests.
     * when the transport layer starts up it will block any incoming requests until
     * this method is called
     */
    final void acceptIncomingRequests() {
        blockIncomingRequestsLatch.countDown();
    }

    /**
     * Returns <code>true</code> iff the given node is already connected.
     */
    boolean nodeConnected(DiscoveryNode node) {
        return isLocalNode(node) || transport.nodeConnected(node);
    }

    /**
     * Connect to the specified node.
     *
     * @param node the node to connect to
     */
    void connectToNode(final DiscoveryNode node) {
        if (isLocalNode(node)) {
            return;
        }
        transport.connectToNode(node, null, (newConnection, actualProfile) ->
                handshake(newConnection, actualProfile.getHandshakeTimeout().millis()));
    }

    /**
     * Executes a high-level handshake using the given connection
     * and returns the discovery node of the node the connection
     * was established with. The handshake will fail if the cluster
     * name on the target node mismatches the local cluster name.
     *
     * @param connection       the connection to a specific node
     * @param handshakeTimeout handshake timeout
     * @return the connected node
     * @throws ConnectTransportException if the connection failed
     * @throws IllegalStateException if the handshake failed
     */
    private DiscoveryNode handshake(final Transport.Connection connection,
                                    final long handshakeTimeout) throws ConnectTransportException {
        return handshake(connection, handshakeTimeout, clusterName::equals);
    }

    /**
     * Executes a high-level handshake using the given connection
     * and returns the discovery node of the node the connection
     * was established with. The handshake will fail if the cluster
     * name on the target node doesn't match the local cluster name.
     *
     * @param connection       the connection to a specific node
     * @param handshakeTimeout handshake timeout
     * @param clusterNamePredicate cluster name validation predicate
     * @return the connected node
     * @throws ConnectTransportException if the connection failed
     * @throws IllegalStateException if the handshake failed
     */
    private DiscoveryNode handshake(final Transport.Connection connection,
                                    final long handshakeTimeout, Predicate<ClusterName> clusterNamePredicate)
            throws ConnectTransportException {
        final HandshakeResponse response;
        final DiscoveryNode node = connection.getNode();
        try {
            PlainTransportFuture<HandshakeResponse> futureHandler = new PlainTransportFuture<>(
                new FutureTransportResponseHandler<HandshakeResponse>() {
                @Override
                public HandshakeResponse newInstance() {
                    return new HandshakeResponse();
                }
            });
            sendRequest(connection, HANDSHAKE_ACTION_NAME, HandshakeRequest.INSTANCE,
                TransportRequestOptions.builder().withTimeout(handshakeTimeout).build(), futureHandler);
            response = futureHandler.txGet();
        } catch (Exception e) {
            throw new IllegalStateException("handshake failed with " + node, e);
        }
        if (!clusterNamePredicate.test(response.clusterName)) {
            throw new IllegalStateException("handshake failed, mismatched cluster name [" +
                    response.clusterName + "] - " + node);
        } else if (!response.version.isCompatible(localNode.getVersion())) {
            throw new IllegalStateException("handshake failed, incompatible version [" +
                    response.version + "] - " + node);
        }
        return response.discoveryNode;
    }

    void disconnectFromNode(DiscoveryNode node) {
        if (isLocalNode(node)) {
            return;
        }
        transport.disconnectFromNode(node);
    }

    <T extends TransportResponse> TransportFuture<T> submitRequest(DiscoveryNode node, String action,
                                                                   TransportRequest request,
                                                                   TransportRequestOptions options,
                                                                   TransportResponseHandler<T> handler)
            throws TransportException {
        PlainTransportFuture<T> futureHandler = new PlainTransportFuture<>(handler);
        try {
            Transport.Connection connection = getConnection(node);
            sendRequest(connection, action, request, options, futureHandler);
        } catch (NodeNotConnectedException ex) {
            futureHandler.handleException(ex);
        }
        return futureHandler;
    }

    final <T extends TransportResponse> void sendRequest(final DiscoveryNode node, final String action,
                                                         final TransportRequest request,
                                                         final TransportRequestOptions options,
                                                         TransportResponseHandler<T> handler) {
        try {
            Transport.Connection connection = getConnection(node);
            sendRequest(connection, action, request, options, handler);
        } catch (NodeNotConnectedException ex) {
            handler.handleException(ex);
        }
    }

    private <T extends TransportResponse> void sendRequest(final Transport.Connection connection, final String action,
                                                                 final TransportRequest request,
                                                                 final TransportRequestOptions options,
                                                                 TransportResponseHandler<T> handler) {

        asyncSender.sendRequest(connection, action, request, options, handler);
    }

    /**
     * Returns either a real transport connection or a local node connection
     * if we are using the local node optimization.
     * @throws NodeNotConnectedException if the given node is not connected
     */
    private Transport.Connection getConnection(DiscoveryNode node) {
        if (isLocalNode(node)) {
            return localNodeConnection;
        } else {
            return transport.getConnection(node);
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private <T extends TransportResponse> void sendRequestInternal(final Transport.Connection connection,
                                                                   final String action,
                                                                   final TransportRequest request,
                                                                   final TransportRequestOptions options,
                                                                   TransportResponseHandler<T> handler) {
        if (connection == null) {
            throw new IllegalStateException("can't send request to a null connection");
        }
        DiscoveryNode node = connection.getNode();
        final long requestId = transport.newRequestId();
        final TimeoutHandler timeoutHandler;
        try {
            if (options.timeout() == null) {
                timeoutHandler = null;
            } else {
                timeoutHandler = new TimeoutHandler(requestId);
            }
            Supplier<ThreadContext.StoredContext> storedContextSupplier =
                    threadPool.getThreadContext().newRestorableContext(true);
            TransportResponseHandler<T> responseHandler =
                    new ContextRestoreResponseHandler<>(storedContextSupplier, handler);
            clientHandlers.put(requestId,
                    new RequestHolder(responseHandler, connection.getNode(), action, timeoutHandler));
            if (lifecycle.stoppedOrClosed()) {
                // if we are not started the exception handling will remove the RequestHolder again
                // and calls the handler to notify the caller. It will only notify if the toStop code
                // hasn't done the work yet.
                throw new TransportException("TransportService is closed stopped can't send request");
            }
            if (timeoutHandler != null) {
                assert options.timeout() != null;
                timeoutHandler.future = threadPool.schedule(options.timeout(), ThreadPool.Names.GENERIC, timeoutHandler);
            }
            connection.sendRequest(requestId, action, request, options);
        } catch (final Exception e) {
            // usually happen either because we failed to connect to the node
            // or because we failed serializing the message
            final RequestHolder<? extends TransportResponse> holderToNotify = clientHandlers.remove(requestId);
            // If holderToNotify == null then handler has already been taken care of.
            if (holderToNotify != null) {
                holderToNotify.cancelTimeout();
                // callback that an exception happened, but on a different thread since we don't
                // want handlers to worry about stack overflows
                final SendRequestTransportException sendRequestException =
                        new SendRequestTransportException(node, action, e);
                threadPool.executor(ThreadPool.Names.GENERIC).execute(new AbstractRunnable() {
                    @Override
                    public void onRejection(Exception e) {
                        // if we get rejected during node shutdown we don't wanna bubble it up
                        logger.debug((Supplier<?>) () -> new ParameterizedMessage(
                                "failed to notify response handler on rejection, action: {}",
                                holderToNotify.action()), e);
                    }
                    @Override
                    public void onFailure(Exception e) {
                        logger.warn((Supplier<?>) () -> new ParameterizedMessage(
                                "failed to notify response handler on exception, action: {}",
                                holderToNotify.action()), e);
                    }
                    @Override
                    protected void doRun() throws Exception {
                        holderToNotify.handler().handleException(sendRequestException);
                    }
                });
            } else {
                logger.debug("Exception while sending request, handler likely already notified due to timeout", e);
            }
        }
    }

    private void sendLocalRequest(long requestId, final String action, final TransportRequest request,
                                  TransportRequestOptions options) {
        final DirectResponseChannel channel = new DirectResponseChannel(logger, localNode, action, requestId, adapter,
                threadPool);
        try {
            adapter.onRequestSent(localNode, requestId, action, request, options);
            adapter.onRequestReceived(requestId, action);
            final RequestHandlerRegistry<TransportRequest> reg = adapter.getRequestHandler(action);
            if (reg == null) {
                throw new ActionNotFoundTransportException("Action [" + action + "] not found");
            }
            final String executor = reg.getExecutor();
            if (ThreadPool.Names.SAME.equals(executor)) {
                reg.processMessageReceived(request, channel);
            } else {
                threadPool.executor(executor).execute(new AbstractRunnable() {
                    @Override
                    protected void doRun() throws Exception {
                        reg.processMessageReceived(request, channel);
                    }

                    @Override
                    public boolean isForceExecution() {
                        return reg.isForceExecution();
                    }

                    @Override
                    public void onFailure(Exception e) {
                        try {
                            channel.sendResponse(e);
                        } catch (Exception inner) {
                            inner.addSuppressed(e);
                            logger.warn((Supplier<?>) () ->
                                    new ParameterizedMessage("failed to notify channel of error message for action [{}]",
                                            action), inner);
                        }
                    }
                });
            }

        } catch (Exception e) {
            try {
                channel.sendResponse(e);
            } catch (Exception inner) {
                inner.addSuppressed(e);
                logger.warn(
                    (Supplier<?>) () -> new ParameterizedMessage(
                        "failed to notify channel of error message for action [{}]", action), inner);
            }
        }
    }

    private boolean shouldTraceAction(String action) {
        if (tracerLogInclude.length > 0) {
            if (!Regex.simpleMatch(tracerLogInclude, action)) {
                return false;
            }
        }
        return tracerLogExclude.length <= 0 || !Regex.simpleMatch(tracerLogExclude, action);
    }

    /**
     * Registers a new request handler.
     *
     * @param action  the action the request handler is associated with
     * @param request the request class that will be used to construct new instances for streaming
     * @param executor the executor the request handling will be executed on
     * @param handler the handler itself that implements the request handling
     */
    private <Request extends TransportRequest> void registerRequestHandler(String action, Supplier<Request> request,
                                                                           String executor,
                                                                           TransportRequestHandler<Request> handler) {
        handler = interceptor.interceptHandler(action, executor, false, handler);
        RequestHandlerRegistry<Request> reg = new RequestHandlerRegistry<>(
            action, request, taskManager, handler, executor, false, false);
        registerRequestHandler(reg);
    }

    @SuppressWarnings("unchecked")
    private <Request extends TransportRequest> void registerRequestHandler(RequestHandlerRegistry<Request> reg) {
        synchronized (requestHandlerMutex) {
            if (requestHandlers.containsKey(reg.getAction())) {
                throw new IllegalArgumentException("transport handlers for action " +
                        reg.getAction() + " is already registered");
            }
            requestHandlers = MapBuilder.newMapBuilder(requestHandlers).put(reg.getAction(),
                    (RequestHandlerRegistry<TransportRequest>) reg).immutableMap();
        }
    }

    private boolean isLocalNode(DiscoveryNode discoveryNode) {
        return Objects.requireNonNull(discoveryNode, "discovery node must not be null").equals(localNode);
    }

    static class HandshakeRequest extends TransportRequest {

        static final HandshakeRequest INSTANCE = new HandshakeRequest();

        private HandshakeRequest() {
        }

    }

    /**
     *
     */
    public static class HandshakeResponse extends TransportResponse {

        private DiscoveryNode discoveryNode;

        private ClusterName clusterName;

        private Version version;

        /**
         * For extern construction.
         */
        public HandshakeResponse() {
        }

        HandshakeResponse(DiscoveryNode discoveryNode, ClusterName clusterName, Version version) {
            this.discoveryNode = discoveryNode;
            this.version = version;
            this.clusterName = clusterName;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            discoveryNode = in.readOptionalWriteable(DiscoveryNode::new);
            clusterName = new ClusterName(in);
            version = Version.readVersion(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeOptionalWriteable(discoveryNode);
            clusterName.writeTo(out);
            Version.writeVersion(version, out);
        }
    }

    private final class Adapter implements TransportServiceAdapter {

        final MeanMetric rxMetric = new MeanMetric();

        final MeanMetric txMetric = new MeanMetric();

        @Override
        public void addBytesReceived(long size) {
            rxMetric.inc(size);
        }

        @Override
        public void addBytesSent(long size) {
            txMetric.inc(size);
        }

        @Override
        public void onRequestSent(DiscoveryNode node, long requestId, String action, TransportRequest request,
                                  TransportRequestOptions options) {
            if (traceEnabled() && shouldTraceAction(action)) {
                traceRequestSent(node, requestId, action, options);
            }
        }

        boolean traceEnabled() {
            return tracerLog.isTraceEnabled();
        }

        @Override
        public void onResponseSent(long requestId, String action, TransportResponse response,
                                   TransportResponseOptions options) {
            if (traceEnabled() && shouldTraceAction(action)) {
                traceResponseSent(requestId, action);
            }
        }

        @Override
        public void onResponseSent(long requestId, String action, Exception e) {
            if (traceEnabled() && shouldTraceAction(action)) {
                traceResponseSent(requestId, action, e);
            }
        }

        void traceResponseSent(long requestId, String action, Exception e) {
            tracerLog.trace(
                (org.apache.logging.log4j.util.Supplier<?>)
                    () -> new ParameterizedMessage("[{}][{}] sent error response", requestId, action), e);
        }

        @Override
        public void onRequestReceived(long requestId, String action) {
            try {
                blockIncomingRequestsLatch.await();
            } catch (InterruptedException e) {
                logger.trace("interrupted while waiting for incoming requests block to be removed");
            }
            if (traceEnabled() && shouldTraceAction(action)) {
                traceReceivedRequest(requestId, action);
            }
        }

        @Override
        public RequestHandlerRegistry<TransportRequest> getRequestHandler(String action) {
            return requestHandlers.get(action);
        }

        @Override
        public TransportResponseHandler<TransportResponse> onResponseReceived(final long requestId) {
            RequestHolder<TransportResponse> holder = clientHandlers.remove(requestId);
            if (holder == null) {
                checkForTimeout(requestId);
                return null;
            }
            holder.cancelTimeout();
            if (traceEnabled() && shouldTraceAction(holder.action())) {
                traceReceivedResponse(requestId, holder.node(), holder.action());
            }
            return holder.handler();
        }

        void checkForTimeout(long requestId) {
            // lets see if its in the timeout holder, but sync on mutex to make sure any ongoing timeout
            // handling has finished
            final DiscoveryNode sourceNode;
            final String action;
            if (clientHandlers.get(requestId) != null) {
                throw new IllegalStateException();
            }
            TimeoutInfoHolder timeoutInfoHolder = timeoutInfoHandlers.remove(requestId);
            if (timeoutInfoHolder != null) {
                long time = System.currentTimeMillis();
                logger.warn("Received response for a request that has timed out, sent [{}ms] ago, timed out [{}ms] ago, " +
                    "action [{}], node [{}], id [{}]", time - timeoutInfoHolder.sentTime(),
                        time - timeoutInfoHolder.timeoutTime(),
                    timeoutInfoHolder.action(), timeoutInfoHolder.node(), requestId);
                action = timeoutInfoHolder.action();
                sourceNode = timeoutInfoHolder.node();
            } else {
                logger.warn("Transport response handler not found of id [{}]", requestId);
                action = null;
                sourceNode = null;
            }
            // call tracer out of lock
            if (!traceEnabled()) {
                return;
            }
            if (action == null) {
                assert sourceNode == null;
                traceUnresolvedResponse(requestId);
            } else if (shouldTraceAction(action)) {
                traceReceivedResponse(requestId, sourceNode, action);
            }
        }

        @Override
        public void onNodeConnected(final DiscoveryNode node) {
        }

        @Override
        public void onConnectionOpened(DiscoveryNode node) {
        }

        @Override
        public void onNodeDisconnected(final DiscoveryNode node) {
            try {
                for (Map.Entry<Long, RequestHolder<TransportResponse>> entry : clientHandlers.entrySet()) {
                    RequestHolder<? extends TransportResponse> holder = entry.getValue();
                    if (holder.node().equals(node)) {
                        final RequestHolder<? extends TransportResponse> holderToNotify = clientHandlers.remove(entry.getKey());
                        if (holderToNotify != null) {
                            // callback that an exception happened, but on a different thread since we don't
                            // want handlers to worry about stack overflows
                            threadPool.generic().execute(() -> holderToNotify.handler()
                                    .handleException(new NodeDisconnectedException(node,
                                holderToNotify.action())));
                        }
                    }
                }
            } catch (EsRejectedExecutionException ex) {
                logger.debug("Rejected execution on NodeDisconnected", ex);
            }
        }

        void traceReceivedRequest(long requestId, String action) {
            tracerLog.trace("[{}][{}] received request", requestId, action);
        }

        void traceResponseSent(long requestId, String action) {
            tracerLog.trace("[{}][{}] sent response", requestId, action);
        }

        void traceReceivedResponse(long requestId, DiscoveryNode sourceNode, String action) {
            tracerLog.trace("[{}][{}] received response from [{}]", requestId, action, sourceNode);
        }

        void traceUnresolvedResponse(long requestId) {
            tracerLog.trace("[{}] received response but can't resolve it to a request", requestId);
        }

        void traceRequestSent(DiscoveryNode node, long requestId, String action, TransportRequestOptions options) {
            tracerLog.trace("[{}][{}] sent to [{}] (timeout: [{}])", requestId, action, node, options.timeout());
        }
    }

    private final class TimeoutHandler implements Runnable {

        private final long requestId;

        private final long sentTime = System.currentTimeMillis();

        volatile ScheduledFuture<?> future;

        TimeoutHandler(long requestId) {
            this.requestId = requestId;
        }

        @Override
        public void run() {
            // we get first to make sure we only add the TimeoutInfoHandler if needed.
            final RequestHolder<? extends TransportResponse> holder = clientHandlers.get(requestId);
            if (holder != null) {
                // add it to the timeout information holder, in case we are going to get a response later
                long timeoutTime = System.currentTimeMillis();
                timeoutInfoHandlers.put(requestId, new TimeoutInfoHolder(holder.node(), holder.action(), sentTime,
                        timeoutTime));
                // now that we have the information visible via timeoutInfoHandlers, we try to remove the request id
                final RequestHolder<? extends TransportResponse> removedHolder = clientHandlers.remove(requestId);
                if (removedHolder != null) {
                    assert removedHolder == holder : "two different holder instances for request [" + requestId + "]";
                    removedHolder.handler().handleException(
                        new ReceiveTimeoutTransportException(holder.node(), holder.action(),
                            "request_id [" + requestId + "] timed out after [" + (timeoutTime - sentTime) + "ms]"));
                } else {
                    // response was processed, remove timeout info.
                    timeoutInfoHandlers.remove(requestId);
                }
            }
        }

        /**
         * Cancels timeout handling. This is a best effort only to avoid running it.
         * Remove the requestId from {@link #clientHandlers} to make sure this doesn't run.
         */
        void cancel() {
            if (clientHandlers.get(requestId) != null) {
                throw new IllegalStateException("cancel must be called after the requestId [" +
                        requestId + "] has been removed from clientHandlers");
            }
            FutureUtils.cancel(future);
        }
    }

    private static class TimeoutInfoHolder {

        private final DiscoveryNode node;
        private final String action;
        private final long sentTime;
        private final long timeoutTime;

        TimeoutInfoHolder(DiscoveryNode node, String action, long sentTime, long timeoutTime) {
            this.node = node;
            this.action = action;
            this.sentTime = sentTime;
            this.timeoutTime = timeoutTime;
        }

        public DiscoveryNode node() {
            return node;
        }

        String action() {
            return action;
        }

        long sentTime() {
            return sentTime;
        }

        long timeoutTime() {
            return timeoutTime;
        }
    }

    private static class RequestHolder<T extends TransportResponse> {

        private final TransportResponseHandler<T> handler;

        private final DiscoveryNode node;

        private final String action;

        private final TimeoutHandler timeoutHandler;

        RequestHolder(TransportResponseHandler<T> handler, DiscoveryNode node, String action,
                      TimeoutHandler timeoutHandler) {
            this.handler = handler;
            this.node = node;
            this.action = action;
            this.timeoutHandler = timeoutHandler;
        }

        TransportResponseHandler<T> handler() {
            return handler;
        }

        public DiscoveryNode node() {
            return this.node;
        }

        String action() {
            return this.action;
        }

        void cancelTimeout() {
            if (timeoutHandler != null) {
                timeoutHandler.cancel();
            }
        }
    }

    /**
     * This handler wrapper ensures that the response thread executes with the correct thread context.
     * Before any of the handle methods are invoked we restore the context.
     * @param <T> thr transport response type
     */
    public static final class ContextRestoreResponseHandler<T extends TransportResponse>
            implements TransportResponseHandler<T> {

        private final TransportResponseHandler<T> delegate;

        private final Supplier<ThreadContext.StoredContext> contextSupplier;

        ContextRestoreResponseHandler(Supplier<ThreadContext.StoredContext> contextSupplier,
                                      TransportResponseHandler<T> delegate) {
            this.delegate = delegate;
            this.contextSupplier = contextSupplier;
        }

        @Override
        public T newInstance() {
            return delegate.newInstance();
        }

        @SuppressWarnings("try")
        @Override
        public void handleResponse(T response) {
            try (ThreadContext.StoredContext ignore = contextSupplier.get()) {
                delegate.handleResponse(response);
            }
        }

        @SuppressWarnings("try")
        @Override
        public void handleException(TransportException exp) {
            try (ThreadContext.StoredContext ignore = contextSupplier.get()) {
                delegate.handleException(exp);
            }
        }

        @Override
        public String executor() {
            return delegate.executor();
        }

        @Override
        public String toString() {
            return getClass().getName() + "/" + delegate.toString();
        }

    }

    static class DirectResponseChannel implements TransportChannel {

        private static final String DIRECT_RESPONSE_PROFILE = ".direct";

        private final Logger logger;

        private final DiscoveryNode localNode;

        private final String action;

        private final long requestId;

        private final TransportServiceAdapter adapter;

        private final ThreadPool threadPool;

        DirectResponseChannel(Logger logger, DiscoveryNode localNode, String action, long requestId,
                                     TransportServiceAdapter adapter, ThreadPool threadPool) {
            this.logger = logger;
            this.localNode = localNode;
            this.action = action;
            this.requestId = requestId;
            this.adapter = adapter;
            this.threadPool = threadPool;
        }

        @Override
        public String action() {
            return action;
        }

        @Override
        public String getProfileName() {
            return DIRECT_RESPONSE_PROFILE;
        }

        @Override
        public void sendResponse(TransportResponse response) throws IOException {
            sendResponse(response, TransportResponseOptions.EMPTY);
        }

        @SuppressWarnings("unchecked")
        @Override
        public void sendResponse(final TransportResponse response, TransportResponseOptions options)
                throws IOException {
            adapter.onResponseSent(requestId, action, response, options);
            final TransportResponseHandler<TransportResponse> handler = adapter.onResponseReceived(requestId);
            if (handler != null) {
                final String executor = handler.executor();
                if (ThreadPool.Names.SAME.equals(executor)) {
                    processResponse(handler, response);
                } else {
                    threadPool.executor(executor).execute(() -> processResponse(handler, response));
                }
            }
        }

        void processResponse(TransportResponseHandler<TransportResponse> handler, TransportResponse response) {
            try {
                handler.handleResponse(response);
            } catch (Exception e) {
                processException(handler, wrapInRemote(new ResponseHandlerFailureTransportException(e)));
            }
        }

        @SuppressWarnings("unchecked")
        @Override
        public void sendResponse(Exception exception) throws IOException {
            adapter.onResponseSent(requestId, action, exception);
            final TransportResponseHandler<TransportResponse> handler = adapter.onResponseReceived(requestId);
            if (handler != null) {
                final RemoteTransportException rtx = wrapInRemote(exception);
                final String executor = handler.executor();
                if (ThreadPool.Names.SAME.equals(executor)) {
                    processException(handler, rtx);
                } else {
                    threadPool.executor(handler.executor()).execute(() -> processException(handler, rtx));
                }
            }
        }

        RemoteTransportException wrapInRemote(Exception e) {
            if (e instanceof RemoteTransportException) {
                return (RemoteTransportException) e;
            }
            return new RemoteTransportException(localNode.getName(), localNode.getAddress(), action, e);
        }

        void processException(final TransportResponseHandler<TransportResponse> handler, final RemoteTransportException rtx) {
            try {
                handler.handleException(rtx);
            } catch (Exception e) {
                logger.error((Supplier<?>) () -> new ParameterizedMessage(
                        "failed to handle exception for action [{}], handler [{}]", action, handler), e);
            }
        }

        @Override
        public long getRequestId() {
            return requestId;
        }

        @Override
        public String getChannelType() {
            return "direct";
        }

        @Override
        public Version getVersion() {
            return localNode.getVersion();
        }
    }
}
