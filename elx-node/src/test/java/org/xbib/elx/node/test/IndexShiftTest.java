package org.xbib.elx.node.test;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.cluster.metadata.AliasAction;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.xbib.elx.api.IndexShiftResult;
import org.xbib.elx.common.ClientBuilder;
import org.xbib.elx.node.NodeAdminClient;
import org.xbib.elx.node.NodeAdminClientProvider;
import org.xbib.elx.node.NodeBulkClient;
import org.xbib.elx.node.NodeBulkClientProvider;
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
        try (NodeAdminClient adminClient = ClientBuilder.builder(helper.client)
                .setAdminClientProvider(NodeAdminClientProvider.class)
                .put(helper.getNodeSettings())
                .build();
             NodeBulkClient bulkClient = ClientBuilder.builder(helper.client)
                .setBulkClientProvider(NodeBulkClientProvider.class)
                     .put(helper.getNodeSettings())
                .build()) {
            Settings settings = Settings.builder()
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 0)
                    .build();
            bulkClient.newIndex("test_shift", settings);
            for (int i = 0; i < 1; i++) {
                bulkClient.index("test_shift", helper.randomString(1), false,
                        "{ \"name\" : \"" + helper.randomString(32) + "\"}");
            }
            bulkClient.flush();
            bulkClient.waitForResponses(30L, TimeUnit.SECONDS);
            IndexShiftResult indexShiftResult =
                    adminClient.shiftIndex("test", "test_shift", Arrays.asList("a", "b", "c"));
            assertTrue(indexShiftResult.getNewAliases().contains("a"));
            assertTrue(indexShiftResult.getNewAliases().contains("b"));
            assertTrue(indexShiftResult.getNewAliases().contains("c"));
            assertTrue(indexShiftResult.getMovedAliases().isEmpty());
            Map<String, String> aliases = adminClient.getAliases("test_shift");
            assertTrue(aliases.containsKey("a"));
            assertTrue(aliases.containsKey("b"));
            assertTrue(aliases.containsKey("c"));
            assertTrue(aliases.containsKey("test"));
            String resolved = adminClient.resolveAlias("test");
            aliases = adminClient.getAliases(resolved);
            assertTrue(aliases.containsKey("a"));
            assertTrue(aliases.containsKey("b"));
            assertTrue(aliases.containsKey("c"));
            assertTrue(aliases.containsKey("test"));
            bulkClient.newIndex("test_shift2", settings);
            for (int i = 0; i < 1; i++) {
                bulkClient.index("test_shift2", helper.randomString(1), false,
                        "{ \"name\" : \"" + helper.randomString(32) + "\"}");
            }
            bulkClient.flush();
            bulkClient.waitForResponses(30L, TimeUnit.SECONDS);
            indexShiftResult = adminClient.shiftIndex("test", "test_shift2", Arrays.asList("d", "e", "f"),
                    (request, index, alias) -> request.addAliasAction(new IndicesAliasesRequest.AliasActions(AliasAction.Type.ADD,
                            index, alias).filter(QueryBuilders.termQuery("my_key", alias)))
            );
            assertTrue(indexShiftResult.getNewAliases().contains("d"));
            assertTrue(indexShiftResult.getNewAliases().contains("e"));
            assertTrue(indexShiftResult.getNewAliases().contains("f"));
            assertTrue(indexShiftResult.getMovedAliases().contains("a"));
            assertTrue(indexShiftResult.getMovedAliases().contains("b"));
            assertTrue(indexShiftResult.getMovedAliases().contains("c"));
            aliases = adminClient.getAliases("test_shift2");
            assertTrue(aliases.containsKey("a"));
            assertTrue(aliases.containsKey("b"));
            assertTrue(aliases.containsKey("c"));
            assertTrue(aliases.containsKey("d"));
            assertTrue(aliases.containsKey("e"));
            assertTrue(aliases.containsKey("f"));
            resolved = adminClient.resolveAlias("test");
            aliases = adminClient.getAliases(resolved);
            assertTrue(aliases.containsKey("a"));
            assertTrue(aliases.containsKey("b"));
            assertTrue(aliases.containsKey("c"));
            assertTrue(aliases.containsKey("d"));
            assertTrue(aliases.containsKey("e"));
            assertTrue(aliases.containsKey("f"));
            if (bulkClient.getBulkController().getLastBulkError() != null) {
                logger.error("error", bulkClient.getBulkController().getLastBulkError());
            }
            assertNull(bulkClient.getBulkController().getLastBulkError());
        }
    }
}
