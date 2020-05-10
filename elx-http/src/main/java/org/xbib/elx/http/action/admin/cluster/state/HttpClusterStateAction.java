package org.xbib.elx.http.action.admin.cluster.state;

import com.carrotsearch.hppc.LongArrayList;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedObjectNotFoundException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.xbib.elx.http.HttpAction;
import org.xbib.netty.http.client.api.Request;
import org.xbib.netty.http.common.HttpResponse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

public class HttpClusterStateAction extends HttpAction<ClusterStateRequest, ClusterStateResponse> {

    private static final String CLUSTER_NAME = "cluster_name";

    private static final String COMPRESSED_SIZE_IN_BYTES = "compressed_size_in_bytes";

    private static final String BLOCKS = "blocks";

    private static final String NODES = "nodes";

    private static final String METADATA = "metadata";

    @Override
    public ClusterStateAction getActionInstance() {
        return ClusterStateAction.INSTANCE;
    }

    @Override
    protected Request.Builder createHttpRequest(String url, ClusterStateRequest request) {
        List<String> list = new ArrayList<>();
        if (request.metaData()) {
            list.add("metadata");
        }
        if (request.blocks()) {
            list.add("blocks");
        }
        if (request.nodes()) {
            list.add("nodes");
        }
        if (request.routingTable()) {
            list.add("routing_table");
        }
        if (request.customs()) {
            list.add("customs");
        }
        if (list.isEmpty()) {
            list.add("_all");
        }
        return newGetRequest(url, "/_cluster/state/" + String.join(",", list)
                + "/" + String.join(",", request.indices()));
    }

    @Override
    protected CheckedFunction<XContentParser, ClusterStateResponse, IOException> entityParser(HttpResponse httpResponse) {
        return this::fromXContent;
    }

    private ClusterStateResponse fromXContent(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.nextToken();
        ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser::getTokenLocation);
        ClusterName clusterName = null;
        ClusterState.Builder builder = null;
        Integer length = null;
        String currentFieldName = parser.currentName();
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if (CLUSTER_NAME.equals(currentFieldName)) {
                    clusterName = new ClusterName(parser.text());
                    builder = ClusterState.builder(clusterName);
                }
                if (COMPRESSED_SIZE_IN_BYTES.equals(currentFieldName)) {
                    length = parser.intValue();
                }
            }
            if (METADATA.equals(currentFieldName)) {
                // MetaData.fromXContent(parser) is broken because of "meta-data"
                parser.nextToken();
                builder.metaData(metadataFromXContent(parser));
            }

        }
        ClusterState clusterState = builder.build();
        return new ClusterStateResponse(clusterName, clusterState, true);
    }

    private MetaData metadataFromXContent(XContentParser parser) throws IOException {
        MetaData.Builder builder = new MetaData.Builder();
        XContentParser.Token token = parser.currentToken();
        String currentFieldName = parser.currentName();
        if (token != XContentParser.Token.START_OBJECT) {
            throw new IllegalArgumentException("Expected a START_OBJECT but got " + token);
        }
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("settings".equals(currentFieldName)) {
                    builder.persistentSettings(Settings.fromXContent(parser));
                } else if ("indices".equals(currentFieldName)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        //IndexMetaData.Builder.fromXContent is broken
                        //builder.put(IndexMetaData.Builder.fromXContent(parser), false);
                        builder.put(indexMetaDataFromXContent(parser), false);
                    }
                } else if ("templates".equals(currentFieldName)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        builder.put(IndexTemplateMetaData.Builder.fromXContent(parser, parser.currentName()));
                    }
                } else if ("index-graveyard".equals(currentFieldName)) {
                    parser.skipChildren();
                } else {
                    try {
                        MetaData.Custom custom = parser.namedObject(MetaData.Custom.class, currentFieldName, null);
                        builder.putCustom(custom.getWriteableName(), custom);
                    } catch (NamedObjectNotFoundException ex) {
                        logger.warn("Skipping unknown custom object with type {}", currentFieldName);
                        parser.skipChildren();
                    }
                }
            } else if (token.isValue()) {
                if ("version".equals(currentFieldName)) {
                    // private field
                    //builder.version = parser.longValue();
                } else if ("cluster_uuid".equals(currentFieldName) || "uuid".equals(currentFieldName)) {
                    // private field
                    //builder.clusterUUID = parser.text();
                } else {
                    throw new IllegalArgumentException("Unexpected field [" + currentFieldName + "]");
                }
            } else {
                throw new IllegalArgumentException("Unexpected token " + token);
            }
        }
        return builder.build();
    }

    private static final String KEY_IN_SYNC_ALLOCATIONS = "in_sync_allocations";
    private static final String KEY_VERSION = "version";
    private static final String KEY_ROUTING_NUM_SHARDS = "routing_num_shards";
    private static final String KEY_SETTINGS = "settings";
    private static final String KEY_STATE = "state";
    private static final String KEY_MAPPINGS = "mappings";
    private static final String KEY_ALIASES = "aliases";
    private  static final String KEY_PRIMARY_TERMS = "primary_terms";

    private IndexMetaData indexMetaDataFromXContent(XContentParser parser) throws IOException {
        if (parser.currentToken() == null) { // fresh parser? move to the first token
            parser.nextToken();
        }
        if (parser.currentToken() == XContentParser.Token.START_OBJECT) {  // on a start object move to next token
            parser.nextToken();
        }
        if (parser.currentToken() != XContentParser.Token.FIELD_NAME) {
            throw new IllegalArgumentException("expected field name but got a " + parser.currentToken());
        }
        IndexMetaData.Builder builder = new IndexMetaData.Builder(parser.currentName());
        String currentFieldName = null;
        XContentParser.Token token = parser.nextToken();
        if (token != XContentParser.Token.START_OBJECT) {
            throw new IllegalArgumentException("expected object but got a " + token);
        }
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (KEY_SETTINGS.equals(currentFieldName)) {
                    builder.settings(Settings.fromXContent(parser));
                } else if (KEY_MAPPINGS.equals(currentFieldName)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            currentFieldName = parser.currentName();
                        } else if (token == XContentParser.Token.START_OBJECT) {
                            Map<String, Object> mappingSource = MapBuilder.<String, Object>newMapBuilder().put(currentFieldName, parser.mapOrdered()).map();
                            builder.putMapping(new MappingMetaData(currentFieldName, mappingSource));
                        } else {
                            throw new IllegalArgumentException("Unexpected token: " + token);
                        }
                    }
                } else if (KEY_ALIASES.equals(currentFieldName)) {
                    while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                        builder.putAlias(AliasMetaData.Builder.fromXContent(parser));
                    }
                } else if (KEY_IN_SYNC_ALLOCATIONS.equals(currentFieldName)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            currentFieldName = parser.currentName();
                        } else if (token == XContentParser.Token.START_ARRAY) {
                            Set<String> allocationIds = new HashSet<>();
                            while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                                if (token == XContentParser.Token.VALUE_STRING) {
                                    allocationIds.add(parser.text());
                                }
                            }
                            builder.putInSyncAllocationIds(Integer.valueOf(currentFieldName), allocationIds);
                        } else {
                            throw new IllegalArgumentException("Unexpected token: " + token);
                        }
                    }
                } else if (KEY_PRIMARY_TERMS.equals(currentFieldName)) {
                    parser.skipChildren(); // TODO
                } else {
                    throw new IllegalArgumentException("Unexpected field for an object " + currentFieldName);
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (KEY_MAPPINGS.equals(currentFieldName)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        if (token == XContentParser.Token.VALUE_EMBEDDED_OBJECT) {
                            builder.putMapping(new MappingMetaData(new CompressedXContent(parser.binaryValue())));
                        } else {
                            Map<String, Object> mapping = parser.mapOrdered();
                            if (mapping.size() == 1) {
                                String mappingType = mapping.keySet().iterator().next();
                                builder.putMapping(new MappingMetaData(mappingType, mapping));
                            }
                        }
                    }
                } else if (KEY_PRIMARY_TERMS.equals(currentFieldName)) {
                    LongArrayList list = new LongArrayList();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        if (token == XContentParser.Token.VALUE_NUMBER) {
                            list.add(parser.longValue());
                        } else {
                            throw new IllegalStateException("found a non-numeric value under [" + KEY_PRIMARY_TERMS + "]");
                        }
                    }
                    // private fiels
                    //builder.primaryTerms(list.toArray());
                } else if (KEY_ALIASES.equals(currentFieldName)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        builder.putAlias(new AliasMetaData.Builder(parser.text()).build());
                    }
                } else {
                    throw new IllegalArgumentException("Unexpected field for an array " + currentFieldName);
                }
            } else if (token.isValue()) {
                if (KEY_STATE.equals(currentFieldName)) {
                    builder.state(IndexMetaData.State.fromString(parser.text()));
                } else if (KEY_VERSION.equals(currentFieldName)) {
                    builder.version(parser.longValue());
                } else if (KEY_ROUTING_NUM_SHARDS.equals(currentFieldName)) {
                    builder.setRoutingNumShards(parser.intValue());
                } else {
                    throw new IllegalArgumentException("Unexpected field [" + currentFieldName + "]");
                }
            } else {
                throw new IllegalArgumentException("Unexpected token " + token);
            }
        }
        return builder.build();
    }
}
