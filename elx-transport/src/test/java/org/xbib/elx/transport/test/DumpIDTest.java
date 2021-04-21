package org.xbib.elx.transport.test;

import org.elasticsearch.index.query.QueryBuilders;
import org.junit.jupiter.api.Test;
import org.xbib.elx.api.IndexDefinition;
import org.xbib.elx.common.ClientBuilder;
import org.xbib.elx.common.DefaultIndexDefinition;
import org.xbib.elx.transport.TransportSearchClient;
import org.xbib.elx.transport.TransportSearchClientProvider;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Stream;

class DumpIDTest {

    @Test
    void testDump() throws Exception {
        IndexDefinition indexDefinition = new DefaultIndexDefinition("zdb", "zdb");
        try (TransportSearchClient searchClient = ClientBuilder.builder()
                .setSearchClientProvider(TransportSearchClientProvider.class)
                .put("cluster.name", "es2")
                .put("host", "atlas:9302")
                .build();
             BufferedWriter writer = Files.newBufferedWriter(Paths.get("zdb.txt"))) {
            Stream<String> stream = searchClient.getIds(qb -> qb
                            .setIndices(indexDefinition.getIndex())
                            .setQuery(QueryBuilders.matchAllQuery()));
            stream.forEach(id -> {
                try {
                    writer.write(id);
                    writer.newLine();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        }
    }
}
