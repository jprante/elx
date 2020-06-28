package org.xbib.elx.node.test;

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
import org.elasticsearch.client.support.AbstractClient;
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
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

public class TestExtension implements ParameterResolver, BeforeEachCallback, AfterEachCallback {

    private static final Logger logger = LogManager.getLogger("test");

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
        return extensionContext.getParent().isPresent() ?
                extensionContext.getParent().get().getStore(ns).getOrComputeIfAbsent(key + count.incrementAndGet(), key -> create(), Helper.class) : null;
    }

    @Override
    public void beforeEach(ExtensionContext extensionContext) throws Exception {
        Helper helper = extensionContext.getParent().isPresent() ?
                extensionContext.getParent().get().getStore(ns).getOrComputeIfAbsent(key + count.get(), key -> create(), Helper.class) : null;
        Objects.requireNonNull(helper);
        logger.info("starting cluster with helper " + helper + " at " + helper.getHome());
        helper.startNode();
        helper.greenHealth();
        logger.info("cluser name = {}", helper.clusterName());
    }

    @Override
    public void afterEach(ExtensionContext extensionContext) throws Exception {
        Helper helper = extensionContext.getParent().isPresent() ?
                extensionContext.getParent().get().getStore(ns).getOrComputeIfAbsent(key + count.get(), key -> create(), Helper.class) : null;
        Objects.requireNonNull(helper);
        helper.closeNodes();
        deleteFiles(Paths.get(helper.getHome()));
        logger.info("files wiped");
        Thread.sleep(1000L); // let OS commit changes
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

        AbstractClient client;

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
                    .put("name", "elx-client") // for threadpool name
                    .put("cluster.name", getClusterName())
                    .put("path.home", getHome())
                    .build();
        }

        void startNode() {
            buildNode().start();
            NodesInfoRequest nodesInfoRequest = new NodesInfoRequest().transport(true);
            NodesInfoResponse response = client.execute(NodesInfoAction.INSTANCE, nodesInfoRequest).actionGet();
            Object obj = response.iterator().next().getTransport().getAddress().publishAddress();
            if (obj instanceof InetSocketTransportAddress) {
                InetSocketTransportAddress address = (InetSocketTransportAddress) obj;
                host = address.address().getHostName();
                port = address.address().getPort();
                logger.info("host = {} port = {}", host, port);
            }
        }

        String randomString(int len) {
            final char[] buf = new char[len];
            final int n = numbersAndLetters.length - 1;
            for (int i = 0; i < buf.length; i++) {
                buf[i] = numbersAndLetters[random.nextInt(n)];
            }
            return new String(buf);
        }

        Node buildNode() {
            String id = "1";
            Settings nodeSettings = Settings.builder()
                    .put(getNodeSettings())
                    .put("node.name", id)
                    .build();
            node = new MockNode(nodeSettings);
            client = (AbstractClient) node.client();
            return node;
        }

        void closeNodes() {
            if (client != null) {
                logger.info("closing client");
                client.close();
            }
            if (node != null) {
                logger.info("closing all nodes");
                node.close();
            }
        }

        void greenHealth() throws IOException {
            try {
                ClusterHealthResponse healthResponse = client.execute(ClusterHealthAction.INSTANCE,
                        new ClusterHealthRequest().waitForStatus(ClusterHealthStatus.GREEN)
                                .timeout(TimeValue.timeValueSeconds(30))).actionGet();
                if (healthResponse != null && healthResponse.isTimedOut()) {
                    throw new IOException("cluster state is " + healthResponse.getStatus().name()
                            + ", from here on, everything will fail!");
                }
            } catch (ElasticsearchTimeoutException e) {
                throw new IOException("cluster does not respond to health request, cowardly refusing to continue");
            }
        }

        String clusterName() {
            ClusterStateRequest clusterStateRequest = new ClusterStateRequest().all();
            ClusterStateResponse clusterStateResponse =
                    client.execute(ClusterStateAction.INSTANCE, clusterStateRequest).actionGet();
            return clusterStateResponse.getClusterName().value();
        }

        private static final Random random = new Random();

        private static final char[] numbersAndLetters = ("0123456789abcdefghijklmnopqrstuvwxyz").toCharArray();
    }
}
