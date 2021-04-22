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
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TransportClientHelper {

    private static final Logger logger = LogManager.getLogger(TransportClientHelper.class.getName());

    private static final Map<String, ElasticsearchClient> clientMap = new HashMap<>();

    public ElasticsearchClient createClient(Settings settings) {
        String clusterName = settings.get("cluster.name", "elasticsearch");
        return clientMap.computeIfAbsent(clusterName, key -> innerCreateClient(settings));
    }

    public void closeClient(Settings settings) {
        String clusterName = settings.get("cluster.name", "elasticsearch");
        ElasticsearchClient client = clientMap.remove(clusterName);
        if (client != null) {
            if (client instanceof Client) {
                ((Client) client).close();
            }
            client.threadPool().shutdownNow();
        }
    }

    public void init(TransportClient transportClient, Settings settings) {
        Collection<TransportAddress> addrs = findAddresses(settings);
        if (!connect(transportClient, addrs, settings.getAsBoolean("autodiscover", false))) {
            throw new NoNodeAvailableException("no cluster nodes available, check settings = "
                    + settings.toDelimitedString(','));
        }
    }

    private Collection<TransportAddress> findAddresses(Settings settings) {
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
                } catch (IOException e) {
                    logger.warn(e.getMessage(), e);
                }
            } else if (splitHost.length == 1) {
                try {
                    String host = splitHost[0];
                    InetAddress inetAddress = NetworkUtils.resolveInetAddress(host, null);
                    TransportAddress address = new InetSocketTransportAddress(inetAddress, defaultPort);
                    addresses.add(address);
                } catch (IOException e) {
                    logger.warn(e.getMessage(), e);
                }
            } else {
                throw new IllegalArgumentException("invalid hostname specification: " + hostname);
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
                logger.debug("trying to discover all nodes...");
                ClusterStateRequestBuilder clusterStateRequestBuilder =
                        new ClusterStateRequestBuilder(transportClient, ClusterStateAction.INSTANCE);
                ClusterStateResponse clusterStateResponse = clusterStateRequestBuilder.execute().actionGet();
                DiscoveryNodes discoveryNodes = clusterStateResponse.getState().getNodes();
                for (DiscoveryNode discoveryNode : discoveryNodes) {
                    transportClient.addTransportAddress(discoveryNode.getAddress());
                }
                logger.info("after discovery: connected to {}", transportClient.connectedNodes());
            }
            return true;
        }
        return false;
    }

    private ElasticsearchClient innerCreateClient(Settings settings) {
        String systemIdentifier = System.getProperty("os.name")
                + " " + System.getProperty("java.vm.name")
                + " " + System.getProperty("java.vm.vendor")
                + " " + System.getProperty("java.vm.version")
                + " Elasticsearch " + Version.CURRENT.toString();
        logger.info("creating transport client on {} with custom settings {}",
                systemIdentifier, settings.getAsMap());
        // we need to disable dead lock check because we may have mixed node/transport clients
        DefaultChannelFuture.setUseDeadLockChecker(false);
        return TransportClient.builder()
                .settings(getTransportClientSettings(settings))
                .build();
    }

    private Settings getTransportClientSettings(Settings settings) {
        return Settings.builder()
                .put("cluster.name", settings.get("cluster.name", "elasticsearch"))
                .put("path.home", settings.get("path.home", "."))
                .put("processors", settings.getAsInt("processors", Runtime.getRuntime().availableProcessors())) // for thread pool size / worker count
                .put("client.transport.sniff", settings.getAsBoolean("client.transport.sniff", false)) // always disable sniff
                .put("client.transport.nodes_sampler_interval", settings.get("client.transport.nodes_sampler_interval", "10000s")) // ridculous long ping, default is 5 seconds
                .put("client.transport.ping_timeout", settings.get("client.transport.ping_timeout", "10000s")) // ridiculous  ping for unresponsive nodes, defauult is 5 seconds
                .put("client.transport.ignore_cluster_name", settings.getAsBoolean("client.transport.ignore_cluster_name", true)) // connect to any cluster
                .build();
    }
}
