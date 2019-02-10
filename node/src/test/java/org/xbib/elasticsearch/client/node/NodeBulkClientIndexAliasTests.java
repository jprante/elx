package org.xbib.elasticsearch.client.node;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequestBuilder;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.xbib.elasticsearch.client.ClientBuilder;
import org.xbib.elasticsearch.client.IndexAliasAdder;
import org.xbib.elasticsearch.client.SimpleBulkControl;
import org.xbib.elasticsearch.client.SimpleBulkMetric;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

@ThreadLeakFilters(defaultFilters = true, filters = {TestRunnerThreadsFilter.class})
public class NodeBulkClientIndexAliasTests extends ESSingleNodeTestCase {

    private static final Logger logger = LogManager.getLogger(NodeBulkClientIndexAliasTests.class.getName());

    public void testIndexAlias() throws Exception {
        final NodeBulkClient client = ClientBuilder.builder()
                .setMetric(new SimpleBulkMetric())
                .setControl(new SimpleBulkControl())
                .getClient(client(), NodeBulkClient.class);
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
