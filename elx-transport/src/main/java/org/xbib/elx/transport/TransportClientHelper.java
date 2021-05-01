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
import org.xbib.elx.common.Parameters;
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

public class TransportClientHelper {

    private static final Logger logger = LogManager.getLogger(TransportClientHelper.class.getName());

    private static final Map<String, ElasticsearchClient> transportClientMap = new HashMap<>();

    public ElasticsearchClient createClient(Settings settings) {
        String clusterName = settings.get("cluster.name", "elasticsearch");
        return transportClientMap.computeIfAbsent(clusterName, key -> innerCreateClient(settings));
    }

    public void closeClient(Settings settings) {
        String clusterName = settings.get("cluster.name", "elasticsearch");
        ElasticsearchClient client = transportClientMap.remove(clusterName);
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
        final int defaultPort = settings.getAsInt(Parameters.PORT.getName(), 9300);
        Collection<TransportAddress> addresses = new ArrayList<>();
        for (String hostname : settings.getAsArray(Parameters.HOST.getName())) {
            String[] splitHost = hostname.split(":", 2);
            try {
                if (splitHost.length == 2) {
                    InetAddress inetAddress =
                            NetworkUtils.resolveInetAddress(splitHost[0], null);
                    TransportAddress address =
                            new InetSocketTransportAddress(inetAddress, Integer.parseInt(splitHost[1]));
                    addresses.add(address);
                } else if (splitHost.length == 1) {
                    InetAddress inetAddress =
                            NetworkUtils.resolveInetAddress(splitHost[0], null);
                    TransportAddress address =
                            new InetSocketTransportAddress(inetAddress, defaultPort);
                    addresses.add(address);
                } else {
                    throw new IllegalArgumentException("invalid hostname specification: " + hostname);
                }
            }
            catch (IOException e) {
                logger.warn(e.getMessage(), e);
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
        Settings transportClientSettings = getTransportClientSettings(settings);
        logger.info("creating transport client on {} with settings {}",
                systemIdentifier, transportClientSettings.getAsMap());
        // we need to disable dead lock check because we may have mixed node/transport clients
        DefaultChannelFuture.setUseDeadLockChecker(false);
        return TransportClient.builder()
                .settings(transportClientSettings)
                .build();
    }

    private Settings getTransportClientSettings(Settings settings) {
        return Settings.builder()
                .put(filter(settings, key -> !isPrivateSettings(key)))
                .put("path.home", settings.get("path.home", "."))
                .build();
    }

    private static Settings filter(Settings settings, Predicate<String> predicate) {
        Settings.Builder builder = Settings.settingsBuilder();
        for (Map.Entry<String, String> me : settings.getAsMap().entrySet()) {
            if (predicate.test(me.getKey())) {
                builder.put(me.getKey(), me.getValue());
            }
        }
        return builder.build();
    }

    private static boolean isPrivateSettings(String key) {
        for (Parameters p : Parameters.values()) {
            if (key.equals(p.getName())) {
                return true;
            }
        }
        return false;
    }
}
