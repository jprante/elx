package org.xbib.elasticsearch.client.transport;

import static org.elasticsearch.common.unit.TimeValue.timeValueSeconds;
import static java.util.stream.Collectors.toList;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.Version;
import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionModule;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.admin.cluster.node.liveness.LivenessRequest;
import org.elasticsearch.action.admin.cluster.node.liveness.LivenessResponse;
import org.elasticsearch.action.admin.cluster.node.liveness.TransportLivenessAction;
import org.elasticsearch.client.support.AbstractClient;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.node.InternalSettingsPreparer;
import org.elasticsearch.node.Node;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.FutureTransportResponseHandler;
import org.elasticsearch.transport.TransportRequestOptions;

import java.io.Closeable;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Simplified transport client, without the node sampling and retrying mode like in the mainline version.
 *
 * Configurable connect ping interval setting added.
 */
public class TransportClient extends AbstractClient {

    private static final Logger logger = LogManager.getLogger(TransportClient.class);

    private static final String CLIENT_TYPE = "transport";

    private final Injector injector;

    private final long pingTimeout;

    private final ClusterName clusterName;

    private final TransportService transportService;

    private final AtomicInteger tempNodeId = new AtomicInteger();

    private final AtomicInteger nodeCounter = new AtomicInteger();

    private final Object mutex = new Object();

    private volatile List<DiscoveryNode> nodes = Collections.emptyList();

    private volatile List<DiscoveryNode> listedNodes = Collections.emptyList();

    private volatile List<DiscoveryNode> filteredNodes = Collections.emptyList();

    private volatile boolean closed;

    /**
     * Creates a new TransportClient with the given settings and plugins.
     * @param settings settings
     */
    public TransportClient(Settings settings) {
        this(buildParams(settings, Settings.EMPTY, Collections.emptyList()));
    }

    /**
     * Creates a new TransportClient with the given settings and plugins.
     * @param settings settings
     * @param plugins plugins
     */
    public TransportClient(Settings settings, Collection<Class<? extends Plugin>> plugins) {
        this(buildParams(settings, Settings.EMPTY, plugins));
    }

    /**
     * Creates a new TransportClient with the given settings, defaults and plugins.
     * @param settings the client settings
     * @param defaultSettings default settings that are merged after the plugins have added it's additional settings.
     * @param plugins the client plugins
     */
    protected TransportClient(Settings settings, Settings defaultSettings, Collection<Class<? extends Plugin>> plugins) {
        this(buildParams(settings, defaultSettings, plugins));
    }

    private TransportClient(final Injector injector) {
        super(getSettings(injector), getThreadPool(injector));
        this.injector = injector;
        this.clusterName = new ClusterName(getSettings(injector).get("cluster.name", "elasticsearch"));
        this.transportService = injector.getInstance(TransportService.class);
        this.pingTimeout = this.settings.getAsTime("client.transport.ping_timeout", timeValueSeconds(5)).millis();
    }

    private static Settings getSettings(Injector injector) {
        return injector.getInstance(Settings.class);
    }

    private static ThreadPool getThreadPool(Injector injector) {
        return injector.getInstance(ThreadPool.class);
    }

    /**
     * Returns the current registered transport addresses to use.
     *
     * @return list of transport addresess
     */
    public List<TransportAddress> transportAddresses() {
        List<TransportAddress> list = new ArrayList<>();
        for (DiscoveryNode listedNode : listedNodes) {
            list.add(listedNode.getAddress());
        }
        return Collections.unmodifiableList(list);
    }

    /**
     * Returns the current connected transport nodes that this client will use.
     * The nodes include all the nodes that are currently alive based on the transport
     * addresses provided.
     *
     * @return list of nodes
     */
    public List<DiscoveryNode> connectedNodes() {
        return this.nodes;
    }

    /**
     * The list of filtered nodes that were not connected to, for example, due to mismatch in cluster name.
     *
     * @return list of nodes
     */
    public List<DiscoveryNode> filteredNodes() {
        return this.filteredNodes;
    }

    /**
     * Returns the listed nodes in the transport client, once added to it.
     *
     * @return list of nodes
     */
    public List<DiscoveryNode> listedNodes() {
        return this.listedNodes;
    }

    /**
     * Adds a list of transport addresses that will be used to connect to.
     * The Node this transport address represents will be used if its possible to connect to it.
     * If it is unavailable, it will be automatically connected to once it is up.
     * In order to get the list of all the current connected nodes, please see {@link #connectedNodes()}.
     *
     * @param discoveryNodes nodes
     * @return this transport client
     */
    public TransportClient addDiscoveryNodes(DiscoveryNodes discoveryNodes) {
        Collection<TransportAddress> addresses = new ArrayList<>();
        for (DiscoveryNode discoveryNode : discoveryNodes) {
            addresses.add(discoveryNode.getAddress());
        }
        addTransportAddresses(addresses);
        return this;
    }

    /**
     * Adds a list of transport addresses that will be used to connect to.
     * The Node this transport address represents will be used if its possible to connect to it.
     * If it is unavailable, it will be automatically connected to once it is up.
     * In order to get the list of all the current connected nodes, please see {@link #connectedNodes()}.
     *
     * @param transportAddresses transport addresses
     */
    public TransportClient addTransportAddresses(Collection<TransportAddress> transportAddresses) {
        synchronized (mutex) {
            if (closed) {
                throw new IllegalStateException("transport client is closed, can't add addresses");
            }
            Set<DiscoveryNode> discoveryNodeList = new HashSet<>(listedNodes);
            logger.info("before adding: nodes={} listednodes={} transportAddresses={}",
                    nodes, listedNodes, transportAddresses);
            for (TransportAddress newTransportAddress : transportAddresses) {
                boolean found = false;
                for (DiscoveryNode discoveryNode : discoveryNodeList) {
                    logger.debug("checking existing address [{}] against new [{}]",
                            discoveryNode.getAddress(), newTransportAddress);
                    if (discoveryNode.getAddress().equals(newTransportAddress)) { // sameHost
                        found = true;
                        logger.debug("address [{}] already connected, ignoring", newTransportAddress, discoveryNode);
                        break;
                    }
                }
                if (!found) {
                    DiscoveryNode node = new DiscoveryNode("#transport#-" + tempNodeId.incrementAndGet(),
                            newTransportAddress, Version.CURRENT.minimumCompatibilityVersion());
                    logger.info("adding new address [{}]", node);
                    discoveryNodeList.add(node);
                }
            }
            listedNodes = Collections.unmodifiableList(new ArrayList<>(discoveryNodeList));
            connect();
        }
        return this;
    }

    /**
     * Removes a transport address from the list of transport addresses that are used to connect to.
     *
     * @param transportAddress transport address to remove
     * @return this transport client
     */
    public TransportClient removeTransportAddress(TransportAddress transportAddress) {
        synchronized (mutex) {
            if (closed) {
                throw new IllegalStateException("transport client is closed, can't remove an address");
            }
            List<DiscoveryNode> builder = new ArrayList<>();
            for (DiscoveryNode otherNode : listedNodes) {
                if (!otherNode.getAddress().equals(transportAddress)) {
                    builder.add(otherNode);
                } else {
                    logger.debug("removing address [{}]", otherNode);
                }
            }
            listedNodes = Collections.unmodifiableList(builder);
        }
        return this;
    }

    @Override
    @SuppressWarnings("rawtypes")
    public void close() {
        synchronized (mutex) {
            if (closed) {
                return;
            }
            closed = true;
            logger.info("disconnecting from nodes {}", nodes);
            for (DiscoveryNode node : nodes) {
                transportService.disconnectFromNode(node);
            }
            nodes = Collections.emptyList();
            logger.info("disconnecting from listed nodes {}", listedNodes);
            for (DiscoveryNode listedNode : listedNodes) {
                transportService.disconnectFromNode(listedNode);
            }
            listedNodes = Collections.emptyList();
        }
        transportService.close();
        PluginsService pluginsService = injector.getInstance(PluginsService.class);
        for (Class<? extends LifecycleComponent> guiceService : pluginsService.getGuiceServiceClasses()) {
            logger.info("closing plugin service {}", guiceService);
            injector.getInstance(guiceService).close();
        }
        // closing all plugins
        pluginsService.filterPlugins(Plugin.class).forEach(plugin -> {
            try {
                logger.info("closing plugin {}", plugin);
                plugin.close();
            } catch (IOException e) {
                logger.warn(e.getMessage(), e);
            }
        });
        try {
            ThreadPool.terminate(injector.getInstance(ThreadPool.class), 10, TimeUnit.SECONDS);
        } catch (Exception e) {
            logger.warn(e.getMessage(), e);
        }
    }

    private void connect() {
        Set<DiscoveryNode> newNodes = new HashSet<>();
        Set<DiscoveryNode> newFilteredNodes = new HashSet<>();
        for (DiscoveryNode listedNode : listedNodes) {
            if (!transportService.nodeConnected(listedNode)) {
                try {
                    logger.info("connecting to listed node " + listedNode);
                    transportService.connectToNode(listedNode);
                } catch (Exception e) {
                    logger.warn("failed to connect to node " + listedNode, e);
                    continue;
                }
            }
            try {
                LivenessResponse livenessResponse = transportService.submitRequest(listedNode,
                        TransportLivenessAction.NAME, new LivenessRequest(),
                        TransportRequestOptions.builder().withType(TransportRequestOptions.Type.STATE)
                                .withTimeout(pingTimeout).build(),
                        new FutureTransportResponseHandler<LivenessResponse>() {
                            @SuppressWarnings("deprecation")
                            @Override
                            public LivenessResponse newInstance() {
                                return new LivenessResponse();
                            }
                        }).txGet();
                if (!clusterName.equals(livenessResponse.getClusterName())) {
                    logger.warn("node {} not part of the cluster {}, ignoring", listedNode, clusterName);
                    newFilteredNodes.add(listedNode);
                } else if (livenessResponse.getDiscoveryNode() != null) {
                    DiscoveryNode nodeWithInfo = livenessResponse.getDiscoveryNode();
                    newNodes.add(new DiscoveryNode(nodeWithInfo.getName(), nodeWithInfo.getId(),
                            nodeWithInfo.getEphemeralId(), nodeWithInfo.getHostName(),
                            nodeWithInfo.getHostAddress(), listedNode.getAddress(), nodeWithInfo.getAttributes(),
                            nodeWithInfo.getRoles(), nodeWithInfo.getVersion()));
                } else {
                    logger.debug("node {} didn't return any discovery info, temporarily using transport discovery node",
                            listedNode);
                    newNodes.add(listedNode);
                }
            } catch (Exception e) {
                logger.warn("failed to get node info for {}, disconnecting", e, listedNode);
                transportService.disconnectFromNode(listedNode);
            }
        }
        for (Iterator<DiscoveryNode> it = newNodes.iterator(); it.hasNext(); ) {
            DiscoveryNode node = it.next();
            if (!transportService.nodeConnected(node)) {
                try {
                    logger.debug("connecting to new node [{}]", node);
                    transportService.connectToNode(node);
                } catch (Exception e) {
                    it.remove();
                    logger.warn("failed to connect to new node [" + node + "], removed", e);
                }
            }
        }
        this.nodes = Collections.unmodifiableList(new ArrayList<>(newNodes));
        logger.info("connected to nodes: {}", nodes);
        this.filteredNodes = Collections.unmodifiableList(new ArrayList<>(newFilteredNodes));
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    protected <R extends ActionRequest, S extends ActionResponse, T extends ActionRequestBuilder<R, S, T>>
    void doExecute(Action<R, S, T> action, final R request, final ActionListener<S> listener) {
        List<DiscoveryNode> nodeList = this.nodes;
        if (nodeList.isEmpty()) {
            throw new NoNodeAvailableException("none of the configured nodes are available: " + this.listedNodes);
        }
        int index = nodeCounter.incrementAndGet();
        if (index < 0) {
            index = 0;
            nodeCounter.set(0);
        }
        DiscoveryNode discoveryNode = nodeList.get(index % nodeList.size());
        // try once and never more
        try {
            ActionRequestValidationException validationException = request.validate();
            if (validationException != null) {
                listener.onFailure(validationException);
                return;
            }
            TransportRequestOptions transportOptions = action.transportOptions(settings);
            transportService.sendRequest(discoveryNode, action.name(), request, transportOptions,
                    new ActionListenerResponseHandler<>(listener, action::newResponse));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private static Injector buildParams(Settings givenSettings, Settings defaultSettings,
                                                     Collection<Class<? extends Plugin>> plugins) {
        Settings providedSettings = givenSettings;
        if (!Node.NODE_NAME_SETTING.exists(providedSettings)) {
            providedSettings = Settings.builder().put(providedSettings)
                    .put(Node.NODE_NAME_SETTING.getKey(), "_client_")
                    .build();
        }
        final PluginsService pluginsService = newPluginService(providedSettings, plugins);
        final Settings settings = Settings.builder().put(defaultSettings).put(pluginsService.updatedSettings()).build();
        final List<Closeable> resourcesToClose = new ArrayList<>();
        final ThreadPool threadPool = new ThreadPool(settings);
        resourcesToClose.add(() -> ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS));
        final NetworkService networkService = new NetworkService(Collections.emptyList());
        try {
            final List<Setting<?>> additionalSettings = new ArrayList<>(pluginsService.getPluginSettings());
            final List<String> additionalSettingsFilter = new ArrayList<>(pluginsService.getPluginSettingsFilter());
            for (final ExecutorBuilder<?> builder : threadPool.builders()) {
                additionalSettings.addAll(builder.getRegisteredSettings());
            }
            SettingsModule settingsModule = new SettingsModule(settings, additionalSettings, additionalSettingsFilter);
            SearchModule searchModule = new SearchModule(settings, true,
                    pluginsService.filterPlugins(SearchPlugin.class));
            List<NamedWriteableRegistry.Entry> entries = new ArrayList<>();
            entries.addAll(NetworkModule.getNamedWriteables());
            entries.addAll(searchModule.getNamedWriteables());
            entries.addAll(pluginsService.filterPlugins(Plugin.class).stream()
                    .flatMap(p -> p.getNamedWriteables().stream())
                    .collect(toList()));
            NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(entries);
            NamedXContentRegistry xContentRegistry = new NamedXContentRegistry(Stream.of(
                    searchModule.getNamedXContents().stream(),
                    pluginsService.filterPlugins(Plugin.class).stream()
                            .flatMap(p -> p.getNamedXContent().stream())
            ).flatMap(Function.identity()).collect(toList()));
            ModulesBuilder modules = new ModulesBuilder();
            // plugin modules must be added here, before others or we can get crazy injection errors
            for (Module pluginModule : pluginsService.createGuiceModules()) {
                modules.add(pluginModule);
            }
            modules.add(b -> b.bind(ThreadPool.class).toInstance(threadPool));
            ActionModule actionModule = new ActionModule(true, settings, null,
                    settingsModule.getIndexScopedSettings(),
                    settingsModule.getClusterSettings(),
                    settingsModule.getSettingsFilter(),
                    threadPool,
                    pluginsService.filterPlugins(ActionPlugin.class), null, null, null);
            modules.add(actionModule);
            CircuitBreakerService circuitBreakerService = Node.createCircuitBreakerService(settingsModule.getSettings(),
                    settingsModule.getClusterSettings());
            PageCacheRecycler pageCacheRecycler = new PageCacheRecycler(settings);
            BigArrays bigArrays = new BigArrays(pageCacheRecycler, circuitBreakerService);
            resourcesToClose.add(circuitBreakerService);
            resourcesToClose.add(bigArrays);
            modules.add(settingsModule);
            NetworkModule networkModule = new NetworkModule(settings, true,
                    pluginsService.filterPlugins(NetworkPlugin.class), threadPool,
                    bigArrays, pageCacheRecycler, circuitBreakerService, namedWriteableRegistry,
                    xContentRegistry, networkService, null);
            final Transport transport = networkModule.getTransportSupplier().get();
            final TransportService transportService = new TransportService(settings, transport, threadPool,
                    networkModule.getTransportInterceptor(),
                    boundTransportAddress -> DiscoveryNode.createLocal(settings, new TransportAddress(TransportAddress.META_ADDRESS, 0),
                            UUIDs.randomBase64UUID()), null, Collections.emptySet());
            modules.add((b -> {
                b.bind(BigArrays.class).toInstance(bigArrays);
                b.bind(PluginsService.class).toInstance(pluginsService);
                b.bind(CircuitBreakerService.class).toInstance(circuitBreakerService);
                b.bind(NamedWriteableRegistry.class).toInstance(namedWriteableRegistry);
                b.bind(Transport.class).toInstance(transport);
                b.bind(TransportService.class).toInstance(transportService);
                b.bind(NetworkService.class).toInstance(networkService);
            }));
            Injector injector = modules.createInjector();
            List<LifecycleComponent> pluginLifecycleComponents = pluginsService.getGuiceServiceClasses()
                    .stream().map(injector::getInstance).collect(Collectors.toList());
            resourcesToClose.addAll(pluginLifecycleComponents);
            transportService.start();
            transportService.acceptIncomingRequests();
            resourcesToClose.clear();
            return injector;
        } finally {
            IOUtils.closeWhileHandlingException(resourcesToClose);
        }
    }

    private static TransportAddress dummyAddress(NetworkModule networkModule) {
        final TransportAddress address;
        try {
            address = networkModule.getTransportSupplier().get().addressesFromString("0.0.0.0:0", 1)[0];
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
        return address;
    }

    private static PluginsService newPluginService(final Settings settings, Collection<Class<? extends Plugin>> plugins) {
        final Settings.Builder settingsBuilder = Settings.builder()
                .put(TcpTransport.PING_SCHEDULE.getKey(), "5s") // enable by default the transport schedule ping interval
                .put(NetworkService.NETWORK_SERVER.getKey(), false)
                .put(CLIENT_TYPE_SETTING_S.getKey(), CLIENT_TYPE);
        if (!settings.isEmpty()) {
            logger.info(settings.toString());
            settingsBuilder.put(InternalSettingsPreparer.prepareSettings(settings));
        }
        return new PluginsService(settingsBuilder.build(), null, null, null, plugins);
    }
}
