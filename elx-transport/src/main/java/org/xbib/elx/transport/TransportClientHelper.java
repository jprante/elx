package org.xbib.elx.transport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequestBuilder;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.jboss.netty.channel.DefaultChannelFuture;
import org.xbib.elx.common.util.NetworkUtils;
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class TransportClientHelper {

    private static final Logger logger = LogManager.getLogger(TransportClientHelper.class.getName());

    private static ElasticsearchClient client;

    private static Object configurationObject;

    private final Object lock = new Object();

    public ElasticsearchClient createClient(Settings settings, Object object) {
        if (configurationObject == null && object != null) {
            configurationObject = object;
        }
        if (configurationObject instanceof ElasticsearchClient) {
            return (ElasticsearchClient) configurationObject;
        }
        if (client == null) {
            synchronized (lock) {
                String systemIdentifier = System.getProperty("os.name")
                        + " " + System.getProperty("java.vm.name")
                        + " " + System.getProperty("java.vm.vendor")
                        + " " + System.getProperty("java.vm.version")
                        + " Elasticsearch " + Version.CURRENT.toString();
                Settings effectiveSettings = Settings.builder()
                        // for thread pool size
                        .put("processors",
                                settings.getAsInt("processors", Runtime.getRuntime().availableProcessors()))
                        .put("client.transport.sniff", false) // do not sniff
                        .put("client.transport.nodes_sampler_interval", "1m") // do not ping
                        .put("client.transport.ping_timeout", "1m") // wait for unresponsive nodes a very long time before disconnect
                        .put("client.transport.ignore_cluster_name", true) // connect to any cluster
                        // custom settings may override defaults
                        .put(settings)
                        .build();
                logger.info("creating transport client on {} with custom settings {} and effective settings {}",
                        systemIdentifier, settings.getAsMap(), effectiveSettings.getAsMap());

                // we need to disable dead lock check because we may have mixed node/transport clients
                DefaultChannelFuture.setUseDeadLockChecker(false);
                client = TransportClient.builder().settings(effectiveSettings).build();
            }
        }
        return client;
    }

    public void closeClient() {
        synchronized (lock) {
            if (client != null) {
                if (client instanceof Client) {
                    ((Client) client).close();
                }
                if (client != null) {
                    client.threadPool().shutdownNow();
                }
                client = null;
            }
        }
    }

    public void init(TransportClient transportClient, Settings settings) throws IOException {
        Collection<TransportAddress> addrs = findAddresses(settings);
        if (!connect(transportClient, addrs, settings.getAsBoolean("autodiscover", false))) {
            throw new NoNodeAvailableException("no cluster nodes available, check settings "
                    + settings.toString());
        }
    }

    private Collection<TransportAddress> findAddresses(Settings settings) throws IOException {
        final int defaultPort = settings.getAsInt("port", 9300);
        Collection<TransportAddress> addresses = new ArrayList<>();
        for (String hostname : settings.getAsArray("host")) {
            String[] splitHost = hostname.split(":", 2);
            if (splitHost.length == 2) {
                try {
                    String host = splitHost[0];
                    InetAddress inetAddress = NetworkUtils.resolveInetAddress(host, null);
                    int port = Integer.parseInt(splitHost[1]);
                    TransportAddress address = new InetSocketTransportAddress(inetAddress, port);
                    addresses.add(address);
                } catch (NumberFormatException e) {
                    logger.warn(e.getMessage(), e);
                }
            }
            if (splitHost.length == 1) {
                String host = splitHost[0];
                InetAddress inetAddress = NetworkUtils.resolveInetAddress(host, null);
                TransportAddress address = new InetSocketTransportAddress(inetAddress, defaultPort);
                addresses.add(address);
            }
        }
        return addresses;
    }

    private boolean connect(TransportClient transportClient, Collection<TransportAddress> addresses, boolean autodiscover) {
        for (TransportAddress address : addresses) {
            transportClient.addTransportAddresses(address);
        }
        List<DiscoveryNode> nodes = transportClient.connectedNodes();
        logger.info("connected to nodes = {}", nodes);
        if (nodes != null && !nodes.isEmpty()) {
            if (autodiscover) {
                logger.debug("trying to auto-discover all nodes...");
                ClusterStateRequestBuilder clusterStateRequestBuilder =
                        new ClusterStateRequestBuilder(transportClient, ClusterStateAction.INSTANCE);
                ClusterStateResponse clusterStateResponse = clusterStateRequestBuilder.execute().actionGet();
                DiscoveryNodes discoveryNodes = clusterStateResponse.getState().getNodes();
                addDiscoveryNodes(transportClient, discoveryNodes);
                logger.info("after auto-discovery: connected to {}", transportClient.connectedNodes());
            }
            return true;
        }
        return false;
    }

    private void addDiscoveryNodes(TransportClient transportClient, DiscoveryNodes discoveryNodes) {
        for (DiscoveryNode discoveryNode : discoveryNodes) {
            transportClient.addTransportAddress(discoveryNode.getAddress());
        }
    }
}
