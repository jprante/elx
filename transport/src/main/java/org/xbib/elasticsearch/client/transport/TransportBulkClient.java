package org.xbib.elasticsearch.client.transport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequestBuilder;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.xbib.elasticsearch.client.AbstractClient;
import org.xbib.elasticsearch.client.BulkControl;
import org.xbib.elasticsearch.client.BulkMetric;
import org.xbib.elasticsearch.client.NetworkUtils;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Transport client with additional methods for bulk processing.
 */
public class TransportBulkClient extends AbstractClient {

    private static final Logger logger = LogManager.getLogger(TransportBulkClient.class.getName());

    public TransportBulkClient init(ElasticsearchClient client, Settings settings, BulkMetric metric, BulkControl control) {
        super.init(client, settings, metric, control);
        // auto-connect here
        try {
            Collection<TransportAddress> addrs = findAddresses(settings);
            if (!connect(addrs, settings.getAsBoolean("autodiscover", false))) {
                throw new NoNodeAvailableException("no cluster nodes available, check settings "
                        + settings.toString());
            }
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
        return this;
    }

    protected ElasticsearchClient createClient(Settings settings) {
        if (settings != null) {
            String version = System.getProperty("os.name")
                    + " " + System.getProperty("java.vm.name")
                    + " " + System.getProperty("java.vm.vendor")
                    + " " + System.getProperty("java.runtime.version")
                    + " " + System.getProperty("java.vm.version");
            logger.info("creating transport client on {} with effective settings {}",
                    version, settings.toString());
            return new TransportClient(Settings.builder()
                    .put(ClusterName.CLUSTER_NAME_SETTING.getKey(), settings.get(ClusterName.CLUSTER_NAME_SETTING.getKey()))
                    .put(EsExecutors.PROCESSORS_SETTING.getKey(), settings.get(EsExecutors.PROCESSORS_SETTING.getKey()))
                    .put("client.transport.ignore_cluster_name", true)
                    .put(NetworkModule.TRANSPORT_TYPE_KEY, Netty4Plugin.NETTY_TRANSPORT_NAME)
                    .build(), Collections.singletonList(Netty4Plugin.class));
        }
        return null;
    }

    @Override
    public synchronized void shutdown() throws IOException {
        super.shutdown();
        logger.info("shutting down...");
        if (client() != null) {
            TransportClient client = (TransportClient) client();
            client.close();
            client.threadPool().shutdown();
        }
        logger.info("shutting down completed");
    }

    private Collection<TransportAddress> findAddresses(Settings settings) throws IOException {
        List<String> hostnames = settings.getAsList("host", Collections.singletonList("localhost"));
        int port = settings.getAsInt("port", 9300);
        Collection<TransportAddress> addresses = new ArrayList<>();
        for (String hostname : hostnames) {
            String[] splitHost = hostname.split(":", 2);
            if (splitHost.length == 2) {
                String host = splitHost[0];
                InetAddress inetAddress = NetworkUtils.resolveInetAddress(host, null);
                try {
                    port = Integer.parseInt(splitHost[1]);
                } catch (Exception e) {
                    logger.warn(e.getMessage(), e);
                }
                addresses.add(new TransportAddress(inetAddress, port));
            }
            if (splitHost.length == 1) {
                String host = splitHost[0];
                InetAddress inetAddress = NetworkUtils.resolveInetAddress(host, null);
                addresses.add(new TransportAddress(inetAddress, port));
            }
        }
        return addresses;
    }

    private boolean connect(Collection<TransportAddress> addresses, boolean autodiscover) {
        logger.info("trying to connect to {}", addresses);
        if (client() == null) {
            throw new IllegalStateException("no client?");
        }
        TransportClient transportClient = (TransportClient) client();
        transportClient.addTransportAddresses(addresses);
        List<DiscoveryNode> nodes = transportClient.connectedNodes();
        logger.info("nodes = {}", nodes);
        if (nodes != null && !nodes.isEmpty()) {
            if (autodiscover) {
                logger.info("trying to auto-discover all cluster nodes...");
                ClusterStateRequestBuilder clusterStateRequestBuilder =
                        new ClusterStateRequestBuilder(client(), ClusterStateAction.INSTANCE);
                ClusterStateResponse clusterStateResponse = clusterStateRequestBuilder.execute().actionGet();
                DiscoveryNodes discoveryNodes = clusterStateResponse.getState().getNodes();
                transportClient.addDiscoveryNodes(discoveryNodes);
                logger.info("after auto-discovery connected to {}", transportClient.connectedNodes());
            }
            return true;
        }
        return false;
    }
}
