package org.elasticsearch.action.admin.indices.get;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.xbib.elx.http.HttpAction;
import org.xbib.netty.http.client.api.Request;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class HttpGetIndexAction extends HttpAction<GetIndexRequest, GetIndexResponse> {

    @Override
    public GetIndexAction getActionInstance() {
        return GetIndexAction.INSTANCE;
    }

    @Override
    protected Request.Builder createHttpRequest(String url, GetIndexRequest getIndexRequest) throws IOException {
        List<String> list =  getIndexRequest.indices().length == 0 ?
                List.of("*") : Arrays.asList(getIndexRequest.indices());
        String command = "/" + String.join(",", list);
        logger.info("command = " + command);
        return newGetRequest(url, command);
    }

    @Override
    protected CheckedFunction<XContentParser, GetIndexResponse, IOException> entityParser() {
        return this::fromXContent;
    }

    @Override
    protected GetIndexResponse emptyResponse() {
        return new GetIndexResponse();
    }

    private GetIndexResponse fromXContent(XContentParser parser) throws IOException {
        ImmutableOpenMap.Builder<String, List<AliasMetaData>> aliases = ImmutableOpenMap.builder();
        ImmutableOpenMap.Builder<String, ImmutableOpenMap<String, MappingMetaData>> mappings = ImmutableOpenMap.builder();
        ImmutableOpenMap.Builder<String, Settings> settings = ImmutableOpenMap.builder();
        List<String> indices = new ArrayList<>();
        if (parser.currentToken() == null) {
            parser.nextToken();
        }
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser::getTokenLocation);
        parser.nextToken();
        while (!parser.isClosed()) {
            if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
                String indexName = parser.currentName();
                indices.add(indexName);
                IndexEntry indexEntry = parseIndexEntry(parser);
                CollectionUtil.timSort(indexEntry.indexAliases, Comparator.comparing(AliasMetaData::alias));
                aliases.put(indexName, Collections.unmodifiableList(indexEntry.indexAliases));
                mappings.put(indexName, indexEntry.indexMappings);
                settings.put(indexName, indexEntry.indexSettings);
            } else if (parser.currentToken() == XContentParser.Token.START_ARRAY) {
                parser.skipChildren();
            } else {
                parser.nextToken();
            }
        }
        return new GetIndexResponse(indices.toArray(new String[0]),
                mappings.build(),
                aliases.build(),
                settings.build());
    }

    private static IndexEntry parseIndexEntry(XContentParser parser) throws IOException {
        List<AliasMetaData> indexAliases = null;
        ImmutableOpenMap<String, MappingMetaData> indexMappings = null;
        Settings indexSettings = null;
        Settings indexDefaultSettings = null;
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.currentToken(), parser::getTokenLocation);
            parser.nextToken();
            if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
                switch (parser.currentName()) {
                    case "aliases":
                        indexAliases = parseAliases(parser);
                        break;
                    case "mappings":
                        indexMappings = parseMappings(parser);
                        break;
                    case "settings":
                        indexSettings = Settings.fromXContent(parser);
                        break;
                    case "defaults":
                        indexDefaultSettings = Settings.fromXContent(parser);
                        break;
                    default:
                        parser.skipChildren();
                }
            } else if (parser.currentToken() == XContentParser.Token.START_ARRAY) {
                parser.skipChildren();
            }
        }
        return new IndexEntry(indexAliases, indexMappings, indexSettings, indexDefaultSettings);
    }

    private static List<AliasMetaData> parseAliases(XContentParser parser) throws IOException {
        List<AliasMetaData> indexAliases = new ArrayList<>();
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.currentToken(), parser::getTokenLocation);
            indexAliases.add(AliasMetaData.Builder.fromXContent(parser));
        }
        return indexAliases;
    }

    private static ImmutableOpenMap<String, MappingMetaData> parseMappings(XContentParser parser) throws IOException {
        ImmutableOpenMap.Builder<String, MappingMetaData> indexMappings = ImmutableOpenMap.builder();
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.currentToken(), parser::getTokenLocation);
            parser.nextToken();
            if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
                String mappingType = parser.currentName();
                indexMappings.put(mappingType, new MappingMetaData(mappingType, parser.map()));
            } else if (parser.currentToken() == XContentParser.Token.START_ARRAY) {
                parser.skipChildren();
            }
        }
        return indexMappings.build();
    }

    private static class IndexEntry {
        List<AliasMetaData> indexAliases = new ArrayList<>();
        ImmutableOpenMap<String, MappingMetaData> indexMappings = ImmutableOpenMap.of();
        Settings indexSettings = Settings.EMPTY;
        Settings indexDefaultSettings = Settings.EMPTY;

        IndexEntry(List<AliasMetaData> indexAliases, ImmutableOpenMap<String, MappingMetaData> indexMappings,
                   Settings indexSettings, Settings indexDefaultSettings) {
            if (indexAliases != null) {
                this.indexAliases = indexAliases;
            }
            if (indexMappings != null) {
                this.indexMappings = indexMappings;
            }
            if (indexSettings != null) {
                this.indexSettings = indexSettings;
            }
            if (indexDefaultSettings != null) {
                this.indexDefaultSettings = indexDefaultSettings;
            }
        }
    }
}
