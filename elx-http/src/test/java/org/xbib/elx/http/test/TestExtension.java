package org.xbib.elx.http.test;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoAction;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.http.HttpInfo;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeValidationException;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.transport.Netty4Plugin;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.xbib.elx.common.Parameters;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Collections;
import java.util.List;
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
        NodesInfoRequest nodesInfoRequest = new NodesInfoRequest()
                .clear()
                .addMetric(NodesInfoRequest.Metric.HTTP.metricName());
        NodesInfoResponse response = helper.client(). execute(NodesInfoAction.INSTANCE, nodesInfoRequest).actionGet();
        TransportAddress address = response.getNodes().get(0).getInfo(HttpInfo.class).getAddress().publishAddress();
        helper.httpHost = address.address().getHostName();
        helper.httpPort = address.address().getPort();
        logger.log(Level.INFO, "http host = " + helper.httpHost + " port = " + helper.httpPort);
    }

    @Override
    public void afterEach(ExtensionContext extensionContext) throws Exception {
        Helper helper = extensionContext.getParent().get().getStore(ns)
                .getOrComputeIfAbsent(key + count.get(), key -> create(), Helper.class);
        closeNodes(helper);
        deleteFiles(Paths.get(helper.getHome()));
        logger.info("data files wiped: " + helper.getHome());
        Thread.sleep(2000L); // let OS commit changes
    }

    private void closeNodes(Helper helper) throws IOException {
        logger.info("closing node");
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
        String home = System.getProperty("path.home", "build/elxhttp/");
        helper.setHome(home + helper.randomString(8));
        helper.setClusterName("test-cluster-" + helper.randomString(8));
        logger.info("cluster: " + helper.getClusterName() + " home: " + helper.getHome());
        return helper;
    }

    static class Helper {

        String home;

        String cluster;

        String httpHost;

        int httpPort;

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

        Settings getClientSettings() {
            return Settings.builder()
                    .put("cluster.name", getClusterName())
                    .put("path.home", getHome())
                    .put(Parameters.HOST.getName(), httpHost)
                    .put(Parameters.PORT.getName(), httpPort)
                    .put(Parameters.CLUSTER_TARGET_HEALTH.getName(), "YELLOW")
                    .put(Parameters.CLUSTER_TARGET_HEALTH_TIMEOUT.getName(), "1m")
                    .put(Parameters.BULK_METRIC_ENABLED.getName(), Boolean.TRUE)
                    .put(Parameters.SEARCH_METRIC_ENABLED.getName(), Boolean.TRUE)
                    .build();
        }

        void startNode() throws NodeValidationException {
            buildNode().start();
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
                    .put("cluster.name", getClusterName())
                    .put("path.home", getHome())
                    .put("node.max_local_storage_nodes", 2)
                    .put("node.master", true)
                    .put("node.data", true)
                    .put("node.name", "1")
                    .put("cluster.initial_master_nodes", "1")
                    .put("discovery.seed_hosts",  "127.0.0.1:9300")
                    .build();
            List<Class<? extends Plugin>> plugins = Collections.singletonList(Netty4Plugin.class);
            this.node = new MockNode(nodeSettings, plugins);
            return node;
        }
    }
}
