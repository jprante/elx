package org.xbib.elx.transport;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequestBuilder;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
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
import org.xbib.elx.common.AbstractExtendedClient;
import org.xbib.elx.common.util.NetworkUtils;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Transport client with additional methods using the BulkProcessor.
 */
public class ExtendedTransportClient extends AbstractExtendedClient {

    private static final Logger logger = LogManager.getLogger(ExtendedTransportClient.class.getName());

    @Override
    protected ElasticsearchClient createClient(Settings settings) throws IOException {
        if (settings != null) {
            String systemIdentifier = System.getProperty("os.name")
                    + " " + System.getProperty("java.vm.name")
                    + " " + System.getProperty("java.vm.vendor")
                    + " " + System.getProperty("java.vm.version")
                    + " Elasticsearch " + Version.CURRENT.toString();
            Settings transportClientSettings = getTransportClientSettings(settings);
            //XContentBuilder settingsBuilder = XContentFactory.jsonBuilder().startObject();
            XContentBuilder effectiveSettingsBuilder = XContentFactory.jsonBuilder().startObject();
            logger.log(Level.INFO, "creating transport client on {} with settings {}",
                    systemIdentifier,
                    //Strings.toString(settings.toXContent(settingsBuilder, ToXContent.EMPTY_PARAMS).endObject()),
                    Strings.toString(transportClientSettings.toXContent(effectiveSettingsBuilder,
                            ToXContent.EMPTY_PARAMS).endObject()));
            return new MyTransportClient(transportClientSettings, Collections.singletonList(Netty4Plugin.class));
        }
        return null;
    }

    @Override
    protected void closeClient() {
        if (getClient() != null) {
            TransportClient client = (TransportClient) getClient();
            client.close();
            client.threadPool().shutdown();
        }
    }

    @Override
    public ExtendedTransportClient init(Settings settings) throws IOException {
        super.init(settings);
        // additional auto-connect
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

    private boolean connect(Collection<TransportAddress> addresses, boolean autodiscover) {
        if (getClient() == null) {
            throw new IllegalStateException("no client present");
        }
        TransportClient transportClient = (TransportClient) getClient();
        for (TransportAddress address : addresses) {
            transportClient.addTransportAddresses(address);
        }
        List<DiscoveryNode> nodes = transportClient.connectedNodes();
        logger.info("connected to nodes = {}", nodes);
        if (nodes != null && !nodes.isEmpty()) {
            if (autodiscover) {
                logger.debug("trying to auto-discover all nodes...");
                ClusterStateRequestBuilder clusterStateRequestBuilder =
                        new ClusterStateRequestBuilder(getClient(), ClusterStateAction.INSTANCE);
                ClusterStateResponse clusterStateResponse = clusterStateRequestBuilder.execute().actionGet();
                DiscoveryNodes discoveryNodes = clusterStateResponse.getState().getNodes();
                addDiscoveryNodes(transportClient, discoveryNodes);
                logger.info("after auto-discovery: connected to {}", transportClient.connectedNodes());
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
                // "processors"
                .put(EsExecutors.PROCESSORS_SETTING.getKey(),
                        settings.get(EsExecutors.PROCESSORS_SETTING.getKey(),
                                String.valueOf(Runtime.getRuntime().availableProcessors())))
                // "transport.type"
                .put(NetworkModule.TRANSPORT_TYPE_KEY,
                        Netty4Plugin.NETTY_TRANSPORT_NAME)
                .build();
    }

    private void addDiscoveryNodes(TransportClient transportClient, DiscoveryNodes discoveryNodes) {
        for (DiscoveryNode discoveryNode : discoveryNodes) {
            transportClient.addTransportAddress(discoveryNode.getAddress());
        }
    }

    static class MyTransportClient extends TransportClient {

        MyTransportClient(Settings settings, Collection<Class<? extends Plugin>> plugins) {
            super(settings, plugins);
        }
    }
}
