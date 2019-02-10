package org.xbib.elasticsearch.client.http;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequestBuilder;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.transport.Netty4Plugin;
import org.junit.Before;
import org.xbib.elasticsearch.client.ClientBuilder;
import org.xbib.elasticsearch.client.IndexAliasAdder;
import org.xbib.elasticsearch.client.SimpleBulkControl;
import org.xbib.elasticsearch.client.SimpleBulkMetric;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@ThreadLeakFilters(defaultFilters = true, filters = {TestRunnerThreadsFilter.class})
public class HttpClientAliasTests extends ESSingleNodeTestCase {

    private static final Logger logger = LogManager.getLogger(HttpClientAliasTests.class.getName());

    private TransportAddress httpAddress;

    @Before
    public void fetchTransportAddress() {
        NodeInfo nodeInfo = client().admin().cluster().prepareNodesInfo().get().getNodes().get(0);
        httpAddress = nodeInfo.getHttp().getAddress().publishAddress();
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Collections.singletonList(Netty4Plugin.class);
    }

    @Override
    public Settings nodeSettings() {
        return Settings.builder()
                .put(super.nodeSettings())
                .put(NetworkModule.TRANSPORT_TYPE_KEY, Netty4Plugin.NETTY_TRANSPORT_NAME)
                .put(NetworkModule.HTTP_TYPE_DEFAULT_KEY, Netty4Plugin.NETTY_HTTP_TRANSPORT_NAME)
                .put(NetworkModule.HTTP_ENABLED.getKey(), true)
                .build();
    }

    private String findHttpAddress() {
        return "http://" + httpAddress.address().getHostName() + ":" + httpAddress.address().getPort();
    }

    public void testIndexAlias() throws Exception {
        final HttpClient client = ClientBuilder.builder()
                .put("urls", findHttpAddress())
                .setMetric(new SimpleBulkMetric())
                .setControl(new SimpleBulkControl())
                .getClient(HttpClient.class);
        try {
            client.newIndex("test1234");
            for (int i = 0; i < 1; i++) {
                client.index("test1234", "test", randomAlphaOfLength(1), false, "{ \"name\" : \"" + randomAlphaOfLength(32) + "\"}");
            }
            client.flushIngest();
            client.refreshIndex("test1234");

            List<String> simpleAliases = Arrays.asList("a", "b", "c");
            client.switchAliases("test", "test1234", simpleAliases);

            client.newIndex("test5678");
            for (int i = 0; i < 1; i++) {
                client.index("test5678", "test", randomAlphaOfLength(1), false, "{ \"name\" : \"" + randomAlphaOfLength(32) + "\"}");
            }
            client.flushIngest();
            client.refreshIndex("test5678");

            simpleAliases = Arrays.asList("d", "e", "f");
            client.switchAliases("test", "test5678", simpleAliases, new IndexAliasAdder() {
                @Override
                public void addIndexAlias(IndicesAliasesRequestBuilder builder, String index, String alias) {
                    builder.addAlias(index, alias, QueryBuilders.termQuery("my_key", alias));
                }
            });
            Map<String, String> aliases = client.getIndexFilters("test5678");
            logger.info("aliases of index test5678 = {}", aliases);

            aliases = client.getAliasFilters("test");
            logger.info("aliases of alias test = {}", aliases);

        } catch (NoNodeAvailableException e) {
            logger.warn("skipping, no node available");
        } finally {
            client.waitForResponses(TimeValue.timeValueSeconds(30));
            client.shutdown();
            if (client.hasThrowable()) {
                logger.error("error", client.getThrowable());
            }
            assertFalse(client.hasThrowable());
        }
    }
}
