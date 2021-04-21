package org.xbib.elx.common.test;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthAction;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoAction;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.node.Node;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

public class TestExtension implements ParameterResolver, BeforeEachCallback, AfterEachCallback {

    private static final Logger logger = LogManager.getLogger("test");

    private static final Random random = new Random();

    private static final char[] numbersAndLetters = ("0123456789abcdefghijklmnopqrstuvwxyz").toCharArray();

    private static final String key = "es-instance-";

    private static final AtomicInteger count = new AtomicInteger(0);

    private static final ExtensionContext.Namespace ns =
            ExtensionContext.Namespace.create(TestExtension.class);

    @Override
    public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
            throws ParameterResolutionException {
        return parameterContext.getParameter().getType().equals(Helper.class);
    }

    @Override
    public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
            throws ParameterResolutionException {
        // initialize new helper here, increase counter
        return extensionContext.getParent().get().getStore(ns)
                .getOrComputeIfAbsent(key + count.incrementAndGet(), key -> create(), Helper.class);
    }

    @Override
    public void beforeEach(ExtensionContext extensionContext) throws Exception {
        Helper helper = extensionContext.getParent().get().getStore(ns)
                .getOrComputeIfAbsent(key + count.get(), key -> create(), Helper.class);
        logger.info("starting cluster with helper " + helper + " at " + helper.getHome());
        helper.startNode();
        try {
            ClusterHealthResponse healthResponse = helper.client().execute(ClusterHealthAction.INSTANCE,
                    new ClusterHealthRequest().waitForStatus(ClusterHealthStatus.GREEN)
                            .timeout(TimeValue.timeValueSeconds(30))).actionGet();
            if (healthResponse != null && healthResponse.isTimedOut()) {
                throw new IOException("cluster state is " + healthResponse.getStatus().name()
                        + ", from here on, everything will fail!");
            }
        } catch (ElasticsearchTimeoutException e) {
            throw new IOException("cluster does not respond to health request, cowardly refusing to continue");
        }
        ClusterStateRequest clusterStateRequest = new ClusterStateRequest().all();
        ClusterStateResponse clusterStateResponse =
                helper.client().execute(ClusterStateAction.INSTANCE, clusterStateRequest).actionGet();
        logger.info("cluster name = {}", clusterStateResponse.getClusterName().value());
    }

    @Override
    public void afterEach(ExtensionContext extensionContext) throws Exception {
        Helper helper = extensionContext.getParent().get().getStore(ns)
                .getOrComputeIfAbsent(key + count.get(), key -> create(), Helper.class);
        closeNodes(helper);
        deleteFiles(Paths.get(helper.getHome()));
        logger.info("files wiped");
        Thread.sleep(2000L); // let OS commit changes
    }

    private void closeNodes(Helper helper) {
        if (helper.node != null) {
            helper.node.close();
        }
        logger.info("all nodes closed");
    }

    private static void deleteFiles(Path directory) throws IOException {
        if (Files.exists(directory)) {
            Files.walkFileTree(directory, new SimpleFileVisitor<>() {
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

    private Helper create() {
        Helper helper = new Helper();
        String home = System.getProperty("path.home", "build/elxnode");
        helper.setHome(home + "/" + helper.randomString(8));
        helper.setClusterName("test-cluster-" + helper.randomString(8));
        logger.info("cluster: " + helper.getClusterName() + " home: " + helper.getHome());
        return helper;
    }

    static class Helper {

        String home;

        String cluster;

        String host;

        int port;

        Node node;

        void setHome(String home) {
            this.home = home;
        }

        String getHome() {
            return home;
        }

        void setClusterName(String cluster) {
            this.cluster = cluster;
        }

        String getClusterName() {
            return cluster;
        }

        Settings getNodeSettings() {
            return Settings.builder()
                    .put("cluster.name", getClusterName())
                    .put("path.home", getHome())
                    .build();
        }

        void startNode() {
            buildNode().start();
            NodesInfoRequest nodesInfoRequest = new NodesInfoRequest().transport(true);
            NodesInfoResponse response = client(). execute(NodesInfoAction.INSTANCE, nodesInfoRequest).actionGet();
            Object obj = response.iterator().next().getTransport().getAddress()
                    .publishAddress();
            if (obj instanceof InetSocketTransportAddress) {
                InetSocketTransportAddress address = (InetSocketTransportAddress) obj;
                host = address.address().getHostName();
                port = address.address().getPort();
            }
        }

        ElasticsearchClient client() {
            return node.client();
        }

        String randomString(int len) {
            final char[] buf = new char[len];
            final int n = numbersAndLetters.length - 1;
            for (int i = 0; i < buf.length; i++) {
                buf[i] = numbersAndLetters[random.nextInt(n)];
            }
            return new String(buf);
        }

        private Node buildNode() {
            Settings nodeSettings = Settings.builder()
                    .put(getNodeSettings())
                    .put("node.name", "1")
                    .build();
            return new MockNode(nodeSettings);
        }
    }
}
