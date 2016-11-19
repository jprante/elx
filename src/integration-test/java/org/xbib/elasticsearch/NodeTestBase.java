package org.xbib.elasticsearch;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthAction;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.client.support.AbstractClient;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.LocalTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.node.MockNode;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeValidationException;
import org.elasticsearch.transport.Netty4Plugin;
import org.junit.After;
import org.junit.Before;
import org.xbib.elasticsearch.extras.client.NetworkUtils;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 */
public class NodeTestBase {

    private static final Logger logger = LogManager.getLogger("test");

    private static final Random random = new Random();

    private static final char[] numbersAndLetters = ("0123456789abcdefghijklmnopqrstuvwxyz").toCharArray();

    private Map<String, Node> nodes = new HashMap<>();

    private Map<String, AbstractClient> clients = new HashMap<>();

    private AtomicInteger counter = new AtomicInteger();

    private String clustername;

    private String host;

    private int port;

    @Before
    public void startNodes() {
        try {
            logger.info("settings cluster name");
            setClusterName();
            logger.info("starting nodes");
            startNode("1");
            findNodeAddress();
            ClusterHealthResponse healthResponse = client("1").execute(ClusterHealthAction.INSTANCE,
                    new ClusterHealthRequest().waitForStatus(ClusterHealthStatus.GREEN)
                            .timeout(TimeValue.timeValueSeconds(30))).actionGet();
            if (healthResponse != null && healthResponse.isTimedOut()) {
                throw new IOException("cluster state is " + healthResponse.getStatus().name()
                        + ", from here on, everything will fail!");
            }
            logger.info("nodes are started");
        } catch (Throwable t) {
            logger.error("start of nodes failed", t);
        }
    }

    @After
    public void stopNodes() {
        try {
            logger.info("stopping nodes");
            closeNodes();
        } catch (Throwable e) {
            logger.error("can not close nodes", e);
        } finally {
            try {
                deleteFiles();
                logger.info("data files wiped");
                Thread.sleep(2000L); // let OS commit changes
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
            } catch (InterruptedException e) {
                // ignore
            }
        }
    }

    protected void setClusterName() {
        this.clustername = "test-helper-cluster-"
                + NetworkUtils.getLocalAddress().getHostName()
                + "-" + System.getProperty("user.name")
                + "-" + counter.incrementAndGet();
    }

    protected String getClusterName() {
        return clustername;
    }

    protected Settings getNodeSettings() {
        String hostname = NetworkUtils.getLocalAddress().getHostName();
        return Settings.builder()
                .put("cluster.name", clustername)
                // required to build a cluster, replica tests will test this.
                .put("discovery.zen.ping.unicast.hosts", hostname)
                .put("transport.type", Netty4Plugin.NETTY_TRANSPORT_NAME)
                .put("network.host", hostname)
                .put("http.enabled", false)
                .put("path.home", getHome())
                // maximum five nodes on same host
                .put("node.max_local_storage_nodes", 5)
                .put("thread_pool.bulk.size", Runtime.getRuntime().availableProcessors())
                // default is 50 which is too low
                .put("thread_pool.bulk.queue_size", 16 * Runtime.getRuntime().availableProcessors())
                .build();
    }


    protected Settings getClientSettings() {
        if (host == null) {
            throw new IllegalStateException("host is null");
        }
        // the host to which transport client should connect to
        return Settings.builder()
                .put("cluster.name", clustername)
                .put("host", host + ":" + port)
                .build();
    }

    protected String getHome() {
        return System.getProperty("path.home");
    }

    public void startNode(String id) throws IOException {
        try {
            buildNode(id).start();
        } catch (NodeValidationException e) {
            throw new IOException(e);
        }
    }

    public AbstractClient client(String id) {
        return clients.get(id);
    }

    private void closeNodes() throws IOException {
        logger.info("closing all clients");
        for (AbstractClient client : clients.values()) {
            client.close();
        }
        clients.clear();
        logger.info("closing all nodes");
        for (Node node : nodes.values()) {
            if (node != null) {
                node.close();
            }
        }
        nodes.clear();
        logger.info("all nodes closed");
    }

    protected void findNodeAddress() {
        NodesInfoRequest nodesInfoRequest = new NodesInfoRequest().transport(true);
        NodesInfoResponse response = client("1").admin().cluster().nodesInfo(nodesInfoRequest).actionGet();
        Object obj = response.getNodes().iterator().next().getTransport().getAddress()
                .publishAddress();
        if (obj instanceof InetSocketTransportAddress) {
            InetSocketTransportAddress address = (InetSocketTransportAddress) obj;
            host = address.address().getHostName();
            port = address.address().getPort();
        } else if (obj instanceof LocalTransportAddress) {
            LocalTransportAddress address = (LocalTransportAddress) obj;
            host = address.getHost();
            port = address.getPort();
        } else {
            logger.info("class=" + obj.getClass());
        }
        if (host == null) {
            throw new IllegalArgumentException("host not found");
        }
    }

    private Node buildNode(String id) throws IOException {
        Settings nodeSettings = Settings.builder()
                .put(getNodeSettings())
                .build();
        logger.info("settings={}", nodeSettings.getAsMap());
        Node node = new MockNode(nodeSettings, Netty4Plugin.class);
        AbstractClient client = (AbstractClient) node.client();
        nodes.put(id, node);
        clients.put(id, client);
        logger.info("clients={}", clients);
        return node;
    }

    protected String randomString(int len) {
        final char[] buf = new char[len];
        final int n = numbersAndLetters.length - 1;
        for (int i = 0; i < buf.length; i++) {
            buf[i] = numbersAndLetters[random.nextInt(n)];
        }
        return new String(buf);
    }

    private static void deleteFiles() throws IOException {
        Path directory = Paths.get(System.getProperty("path.home") + "/data");
        Files.walkFileTree(directory, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                Files.delete(dir);
                return FileVisitResult.CONTINUE;
            }

        });
    }
}
