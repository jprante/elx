package org.xbib.elasticsearch.client.node;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeValidationException;
import org.elasticsearch.plugins.Plugin;
import org.xbib.elasticsearch.client.AbstractClient;
import org.xbib.elasticsearch.client.BulkControl;
import org.xbib.elasticsearch.client.BulkMetric;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

/**
 *
 */
public class NodeBulkClient extends AbstractClient {

    private static final Logger logger = LogManager.getLogger(NodeBulkClient.class.getName());

    private Node node;

    public NodeBulkClient init(ElasticsearchClient client, Settings settings, BulkMetric metric, BulkControl control) {
        super.init(client, settings, metric, control);
        return this;
    }

    @Override
    protected ElasticsearchClient createClient(Settings settings) throws IOException {
        if (settings != null) {
            String version = System.getProperty("os.name")
                    + " " + System.getProperty("java.vm.name")
                    + " " + System.getProperty("java.vm.vendor")
                    + " " + System.getProperty("java.runtime.version")
                    + " " + System.getProperty("java.vm.version");
            Settings effectiveSettings = Settings.builder().put(settings)
                    .put("node.client", true)
                    .put("node.master", false)
                    .put("node.data", false)
                    .build();
            logger.info("creating node client on {} with effective settings {}",
                    version, effectiveSettings.toString());
            Collection<Class<? extends Plugin>> plugins = Collections.emptyList();
            this.node = new BulkNode(new Environment(effectiveSettings, null), plugins);
            try {
                node.start();
            } catch (NodeValidationException e) {
                throw new IOException(e);
            }
            return node.client();
        }
        return null;
    }

    @Override
    public synchronized void shutdown() throws IOException {
        super.shutdown();
        try {
            if (node != null) {
                logger.debug("closing node...");
                node.close();
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    private static class BulkNode extends Node {

        BulkNode(Environment env, Collection<Class<? extends Plugin>> classpathPlugins) {
            super(env, classpathPlugins);
        }
    }
}
