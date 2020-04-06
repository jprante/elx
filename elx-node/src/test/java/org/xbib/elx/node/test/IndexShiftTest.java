package org.xbib.elx.node.test;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.xbib.elx.api.IndexShiftResult;
import org.xbib.elx.common.ClientBuilder;
import org.xbib.elx.node.ExtendedNodeClient;
import org.xbib.elx.node.ExtendedNodeClientProvider;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(TestExtension.class)
class IndexShiftTest {

    private static final Logger logger = LogManager.getLogger(IndexShiftTest.class.getName());

    private final TestExtension.Helper helper;

    IndexShiftTest(TestExtension.Helper helper) {
        this.helper = helper;
    }

    @Test
    void testIndexShift() throws Exception {
        final ExtendedNodeClient client = ClientBuilder.builder(helper.client("1"))
                .provider(ExtendedNodeClientProvider.class)
                .build();
        try {
            Settings settings = Settings.builder()
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 0)
                    .build();
            client.newIndex("test_shift", settings);
            for (int i = 0; i < 1; i++) {
                client.index("test_shift", helper.randomString(1), false,
                        "{ \"name\" : \"" + helper.randomString(32) + "\"}");
            }
            client.flush();
            client.waitForResponses(30L, TimeUnit.SECONDS);
            IndexShiftResult indexShiftResult =
                    client.shiftIndex("test", "test_shift", Arrays.asList("a", "b", "c"));
            assertTrue(indexShiftResult.getNewAliases().contains("a"));
            assertTrue(indexShiftResult.getNewAliases().contains("b"));
            assertTrue(indexShiftResult.getNewAliases().contains("c"));
            assertTrue(indexShiftResult.getMovedAliases().isEmpty());

            Map<String, String> aliases = client.getAliases("test_shift");
            assertTrue(aliases.containsKey("a"));
            assertTrue(aliases.containsKey("b"));
            assertTrue(aliases.containsKey("c"));
            assertTrue(aliases.containsKey("test"));

            String resolved = client.resolveAlias("test");
            aliases = client.getAliases(resolved);
            assertTrue(aliases.containsKey("a"));
            assertTrue(aliases.containsKey("b"));
            assertTrue(aliases.containsKey("c"));
            assertTrue(aliases.containsKey("test"));

            client.newIndex("test_shift2", settings);
            for (int i = 0; i < 1; i++) {
                client.index("test_shift2", helper.randomString(1), false,
                        "{ \"name\" : \"" + helper.randomString(32) + "\"}");
            }
            client.flush();
            client.waitForResponses(30L, TimeUnit.SECONDS);

            indexShiftResult = client.shiftIndex("test", "test_shift2", Arrays.asList("d", "e", "f"),
                    (request, index, alias) -> request.addAliasAction(IndicesAliasesRequest.AliasActions.add()
                            .index(index).alias(alias).filter(QueryBuilders.termQuery("my_key", alias)))
            );
            assertTrue(indexShiftResult.getNewAliases().contains("d"));
            assertTrue(indexShiftResult.getNewAliases().contains("e"));
            assertTrue(indexShiftResult.getNewAliases().contains("f"));
            assertTrue(indexShiftResult.getMovedAliases().contains("a"));
            assertTrue(indexShiftResult.getMovedAliases().contains("b"));
            assertTrue(indexShiftResult.getMovedAliases().contains("c"));

            aliases = client.getAliases("test_shift2");
            assertTrue(aliases.containsKey("a"));
            assertTrue(aliases.containsKey("b"));
            assertTrue(aliases.containsKey("c"));
            assertTrue(aliases.containsKey("d"));
            assertTrue(aliases.containsKey("e"));
            assertTrue(aliases.containsKey("f"));

            resolved = client.resolveAlias("test");
            aliases = client.getAliases(resolved);
            assertTrue(aliases.containsKey("a"));
            assertTrue(aliases.containsKey("b"));
            assertTrue(aliases.containsKey("c"));
            assertTrue(aliases.containsKey("d"));
            assertTrue(aliases.containsKey("e"));
            assertTrue(aliases.containsKey("f"));

        } catch (NoNodeAvailableException e) {
            logger.warn("skipping, no node available");
        } finally {
            client.close();
            if (client.getBulkController().getLastBulkError() != null) {
                logger.error("error", client.getBulkController().getLastBulkError());
            }
            assertNull(client.getBulkController().getLastBulkError());
        }
    }
}
