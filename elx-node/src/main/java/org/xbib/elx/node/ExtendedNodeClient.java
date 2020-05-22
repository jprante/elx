package org.xbib.elx.node;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.node.Node;
import org.elasticsearch.plugins.Plugin;
import org.xbib.elx.common.AbstractExtendedClient;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

public class ExtendedNodeClient extends AbstractExtendedClient {

    private static final Logger logger = LogManager.getLogger(ExtendedNodeClient.class.getName());

    private Node node;

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
                    version, effectiveSettings.getAsMap());
            Collection<Class<? extends Plugin>> plugins = Collections.emptyList();
            this.node = new BulkNode(new Environment(effectiveSettings), plugins);
            try {
                node.start();
            } catch (Exception e) {
                throw new IOException(e);
            }
            return node.client();
        }
        return null;
    }

    @Override
    protected void closeClient() {
        if (node != null) {
            logger.debug("closing node...");
            node.close();
        }
    }

    private static class BulkNode extends Node {

        BulkNode(Environment env, Collection<Class<? extends Plugin>> classpathPlugins) {
            super(env, Version.CURRENT, classpathPlugins);
        }
    }
}
