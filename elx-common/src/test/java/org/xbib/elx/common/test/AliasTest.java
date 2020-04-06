package org.xbib.elx.common.test;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesAction;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesAction;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.Strings;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(TestExtension.class)
class AliasTest {

    private static final Logger logger = LogManager.getLogger(AliasTest.class.getName());

    private final TestExtension.Helper helper;

    AliasTest(TestExtension.Helper helper) {
        this.helper = helper;
     }

    @Test
    void testAlias() {
        ElasticsearchClient client = helper.client("1");
        CreateIndexRequest indexRequest = new CreateIndexRequest("test");
        client.execute(CreateIndexAction.INSTANCE, indexRequest).actionGet();
        // put alias
        IndicesAliasesRequest indicesAliasesRequest = new IndicesAliasesRequest();
        String[] indices = new String[] { "test" };
        String[] aliases = new String[] { "test_alias" };
        IndicesAliasesRequest.AliasActions aliasAction =
                new IndicesAliasesRequest.AliasActions(IndicesAliasesRequest.AliasActions.Type.ADD)
                        .indices(indices)
                        .aliases(aliases);
        indicesAliasesRequest.addAliasAction(aliasAction);
        client.execute(IndicesAliasesAction.INSTANCE, indicesAliasesRequest).actionGet();
        // get alias
        GetAliasesRequest getAliasesRequest = new GetAliasesRequest(Strings.EMPTY_ARRAY);
        long t0 = System.nanoTime();
        GetAliasesResponse getAliasesResponse = client.execute(GetAliasesAction.INSTANCE, getAliasesRequest).actionGet();
        long t1 = (System.nanoTime() - t0) / 1000000;
        logger.info("{} time(ms) = {}", getAliasesResponse.getAliases(), t1);
        assertTrue(t1 >= 0);
    }

    @Test
    void testMostRecentIndex() {
        ElasticsearchClient client = helper.client("1");
        String alias = "test";
        CreateIndexRequest indexRequest = new CreateIndexRequest("test20160101");
        client.execute(CreateIndexAction.INSTANCE, indexRequest).actionGet();
        indexRequest = new CreateIndexRequest("test20160102");
        client.execute(CreateIndexAction.INSTANCE, indexRequest).actionGet();
        indexRequest = new CreateIndexRequest("test20160103");
        client.execute(CreateIndexAction.INSTANCE, indexRequest).actionGet();
        IndicesAliasesRequest indicesAliasesRequest = new IndicesAliasesRequest();
        String[] indices = new String[] { "test20160101", "test20160102", "test20160103" };
        String[] aliases = new String[] { alias };
        IndicesAliasesRequest.AliasActions aliasAction =
                new IndicesAliasesRequest.AliasActions(IndicesAliasesRequest.AliasActions.Type.ADD)
                .indices(indices)
                .aliases(aliases);
        indicesAliasesRequest.addAliasAction(aliasAction);
        client.execute(IndicesAliasesAction.INSTANCE, indicesAliasesRequest).actionGet();
        GetAliasesRequest getAliasesRequest = new GetAliasesRequest();
        getAliasesRequest.aliases(alias);
        GetAliasesResponse getAliasesResponse =
                client.execute(GetAliasesAction.INSTANCE, getAliasesRequest).actionGet();
        Pattern pattern = Pattern.compile("^(.*?)(\\d+)$");
        Set<String> result = new TreeSet<>(Collections.reverseOrder());
        for (ObjectCursor<String> indexName : getAliasesResponse.getAliases().keys()) {
            Matcher m = pattern.matcher(indexName.value);
            if (m.matches()) {
                if (alias.equals(m.group(1))) {
                    result.add(indexName.value);
                }
            }
        }
        Iterator<String> it = result.iterator();
        assertEquals("test20160103", it.next());
        assertEquals("test20160102", it.next());
        assertEquals("test20160101", it.next());
        logger.info("success: result={}", result);
    }
}
