package org.xbib.elx.transport;

import org.apache.logging.log4j.Level;
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
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.transport.Netty4Plugin;
import org.xbib.elx.common.util.NetworkUtils;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Transport client with additional methods using the BulkProcessor.
 */
public class TransportClientHelper {

    private static final Logger logger = LogManager.getLogger(TransportClientHelper.class.getName());

    private static final Map<String, ElasticsearchClient> clientMap = new HashMap<>();

    protected ElasticsearchClient createClient(Settings settings) throws IOException {
        if (settings != null) {
            String systemIdentifier = System.getProperty("os.name")
                    + " " + System.getProperty("java.vm.name")
                    + " " + System.getProperty("java.vm.vendor")
                    + " " + System.getProperty("java.vm.version")
                    + " Elasticsearch " + Version.CURRENT.toString();
            Settings transportClientSettings = getTransportClientSettings(settings);
            XContentBuilder effectiveSettingsBuilder = XContentFactory.jsonBuilder().startObject();
            logger.log(Level.INFO, "creating transport client on {} with settings {}",
                    systemIdentifier,
                    Strings.toString(transportClientSettings.toXContent(effectiveSettingsBuilder,
                            ToXContent.EMPTY_PARAMS).endObject()));
            return new MyTransportClient(transportClientSettings, Collections.singletonList(Netty4Plugin.class));
        }
        return null;
    }

    public void closeClient(Settings settings) {
        ElasticsearchClient client = clientMap.remove(settings.get("cluster.name"));
        if (client != null) {
            if (client instanceof Client) {
                ((Client) client).close();
            }
            client.threadPool().shutdownNow();
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
        for (String hostname : settings.getAsList("host")) {
            String[] splitHost = hostname.split(":", 2);
            if (splitHost.length == 2) {
                try {
                    String host = splitHost[0];
                    InetAddress inetAddress = NetworkUtils.resolveInetAddress(host, null);
                    int port = Integer.parseInt(splitHost[1]);
                    TransportAddress address = new TransportAddress(inetAddress, port);
                    addresses.add(address);
                } catch (NumberFormatException e) {
                    logger.warn(e.getMessage(), e);
                }
            }
            if (splitHost.length == 1) {
                String host = splitHost[0];
                InetAddress inetAddress = NetworkUtils.resolveInetAddress(host, null);
                TransportAddress address = new TransportAddress(inetAddress, defaultPort);
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

    private Settings getTransportClientSettings(Settings settings) {
        return Settings.builder()
                // "cluster.name"
                .put(ClusterName.CLUSTER_NAME_SETTING.getKey(),
                        settings.get(ClusterName.CLUSTER_NAME_SETTING.getKey()))
                // "node.processors"
                .put(EsExecutors.NODE_PROCESSORS_SETTING.getKey(),
                        settings.get(EsExecutors.NODE_PROCESSORS_SETTING.getKey(),
                                String.valueOf(Runtime.getRuntime().availableProcessors())))
                // "transport.type"
                .put(NetworkModule.TRANSPORT_TYPE_KEY,
                        Netty4Plugin.NETTY_TRANSPORT_NAME)
                .build();
    }

    static class MyTransportClient extends TransportClient {

        MyTransportClient(Settings settings, Collection<Class<? extends Plugin>> plugins) {
            super(settings, plugins);
        }
    }
}
