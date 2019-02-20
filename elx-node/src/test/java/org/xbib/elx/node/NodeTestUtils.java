package org.xbib.elx.node;

import static org.elasticsearch.common.settings.Settings.settingsBuilder;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthAction;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.client.support.AbstractClient;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.node.MockNode;
import org.elasticsearch.node.Node;
import org.junit.After;
import org.junit.Before;
import org.xbib.elx.common.util.NetworkUtils;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

public class NodeTestUtils {

    private static final Logger logger = LogManager.getLogger("test");

    private static Random random = new Random();

    private static char[] numbersAndLetters = ("0123456789abcdefghijklmnopqrstuvwxyz").toCharArray();

    private Map<String, Node> nodes = new HashMap<>();

    private Map<String, AbstractClient> clients = new HashMap<>();

    private AtomicInteger counter = new AtomicInteger();

    protected String clusterName;

    private static void deleteFiles() throws IOException {
        Path directory = Paths.get(getHome() + "/data");
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

    @Before
    public void startNodes() {
        try {
            logger.info("starting");
            setClusterName();
            startNode("1");
            try {
                ClusterHealthResponse healthResponse = client("1").execute(ClusterHealthAction.INSTANCE,
                        new ClusterHealthRequest().waitForStatus(ClusterHealthStatus.GREEN)
                                .timeout(TimeValue.timeValueSeconds(30))).actionGet();
                if (healthResponse != null && healthResponse.isTimedOut()) {
                    throw new IOException("cluster state is " + healthResponse.getStatus().name()
                            + ", from here on, everything will fail!");
                }
            } catch (ElasticsearchTimeoutException e) {
                throw new IOException("cluster does not respond to health request, cowardly refusing to continue");
            }
        } catch (Throwable t) {
            logger.error("startNodes failed", t);
        }
    }

    @After
    public void stopNodes() {
        try {
            closeNodes();
        } catch (Exception e) {
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
        this.clusterName = "test-helper-cluster-"
                + NetworkUtils.getLocalAddress().getHostName()
                + "-" + System.getProperty("user.name")
                + "-" + counter.incrementAndGet();
    }

    protected Settings getNodeSettings() {
        return settingsBuilder()
                .put("cluster.name", clusterName)
                .put("cluster.routing.schedule", "50ms")
                .put("cluster.routing.allocation.disk.threshold_enabled", false)
                .put("discovery.zen.multicast.enabled", true)
                .put("discovery.zen.multicast.ping_timeout", "5s")
                .put("http.enabled", true)
                .put("threadpool.bulk.size", Runtime.getRuntime().availableProcessors())
                .put("threadpool.bulk.queue_size", 16 * Runtime.getRuntime().availableProcessors()) // default is 50, too low
                .put("index.number_of_replicas", 0)
                .put("path.home", getHome())
                .build();
    }

    protected static String getHome() {
        return System.getProperty("path.home", System.getProperty("user.dir"));
    }

    public void startNode(String id) {
        buildNode(id).start();
    }

    public AbstractClient client(String id) {
        return clients.get(id);
    }

    private void closeNodes() {
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

    private Node buildNode(String id) {
        Settings nodeSettings = settingsBuilder()
                .put(getNodeSettings())
                .put("name", id)
                .build();
        logger.info("settings={}", nodeSettings.getAsMap());
        Node node = new MockNode(nodeSettings);
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
}