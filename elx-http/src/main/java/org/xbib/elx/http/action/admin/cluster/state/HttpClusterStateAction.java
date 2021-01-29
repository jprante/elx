package org.xbib.elx.http.action.admin.cluster.state;

import com.carrotsearch.hppc.LongArrayList;
import org.apache.logging.log4j.Level;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.rollover.RolloverInfo;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.coordination.CoordinationMetadata;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
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

    private static final String METADATA = "metadata";

    @Override
    public ClusterStateAction getActionInstance() {
        return ClusterStateAction.INSTANCE;
    }

    @Override
    protected Request.Builder createHttpRequest(String url, ClusterStateRequest request) {
        List<String> list = new ArrayList<>();
        if (request.metadata()) {
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
        ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser);
        ClusterName clusterName = null;
        ClusterState.Builder builder = ClusterState.builder(new ClusterName(""));
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
                    parser.intValue();
                }
            }
            if (METADATA.equals(currentFieldName)) {
                parser.nextToken();
                // MetaData.fromXContent(parser) is broken (Expected [meta-data] as a field name but got cluster_uuid)
                // so we have to replace it
                builder.metadata(metadataFromXContent(parser));
            }
        }
        ClusterState clusterState = builder.build();
        return new ClusterStateResponse(clusterName, clusterState, true);
    }

    private Metadata metadataFromXContent(XContentParser parser) throws IOException {
        Metadata.Builder builder = new Metadata.Builder();
        XContentParser.Token token = parser.currentToken();
        String currentFieldName = parser.currentName();
        if (token != XContentParser.Token.START_OBJECT) {
            throw new IllegalArgumentException("Expected a START_OBJECT but got " + token);
        }
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("cluster_coordination".equals(currentFieldName)) {
                    builder.coordinationMetadata(CoordinationMetadata.fromXContent(parser));
                } else if ("settings".equals(currentFieldName)) {
                    builder.persistentSettings(Settings.fromXContent(parser));
                } else if ("indices".equals(currentFieldName)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        builder.put(indexMetaDataFromXContent(parser), false);
                        // builder.put(IndexMetadata.Builder.fromXContent(parser), false);
                    }
                } else if ("hashes_of_consistent_settings".equals(currentFieldName)) {
                    builder.hashesOfConsistentSettings(parser.mapStrings());
                } else if ("templates".equals(currentFieldName)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        builder.put(IndexTemplateMetadata.Builder.fromXContent(parser, parser.currentName()));
                    }
                } else if ("index-graveyard".equals(currentFieldName)) {
                    parser.skipChildren();
                } else {
                    try {
                        Metadata.Custom custom = parser.namedObject(Metadata.Custom.class, currentFieldName, null);
                        builder.putCustom(custom.getWriteableName(), custom);
                    } catch (NamedObjectNotFoundException ex) {
                        logger.warn("Skipping unknown custom object with type {}", currentFieldName);
                        parser.skipChildren();
                    }
                }
            } else if (token.isValue()) {
                if ("version".equals(currentFieldName)) {
                    // private field
                } else if ("cluster_uuid".equals(currentFieldName) || "uuid".equals(currentFieldName)) {
                    // private field
                } else if ("cluster_uuid_committed".equals(currentFieldName)) {
                    // skip
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
    private static final String KEY_MAPPING_VERSION = "mapping_version";
    private static final String KEY_SETTINGS_VERSION = "settings_version";
    private static final String KEY_ALIASES_VERSION = "aliases_version";
    private static final String KEY_ROUTING_NUM_SHARDS = "routing_num_shards";
    private static final String KEY_ROLLOVER_INFOS = "rollover_info";
    private static final String KEY_SYSTEM = "system";
    private static final String KEY_SETTINGS = "settings";
    private static final String KEY_STATE = "state";
    private static final String KEY_MAPPINGS = "mappings";
    private static final String KEY_ALIASES = "aliases";
    private  static final String KEY_PRIMARY_TERMS = "primary_terms";

    private IndexMetadata indexMetaDataFromXContent(XContentParser parser) throws IOException {
        if (parser.currentToken() == null) {
            parser.nextToken();
        }
        if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
            parser.nextToken();
        }
        if (parser.currentToken() != XContentParser.Token.FIELD_NAME) {
            throw new IllegalArgumentException("expected field name but got a " + parser.currentToken());
        }
        IndexMetadata.Builder builder = new IndexMetadata.Builder(parser.currentName());
        String currentFieldName = null;
        XContentParser.Token token = parser.nextToken();
        if (token != XContentParser.Token.START_OBJECT) {
            throw new IllegalArgumentException("expected object but got a " + token);
        }
        boolean mappingVersion = false;
        boolean settingsVersion = false;
        boolean aliasesVersion = false;
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
                            Map<String, Object> mappingSource =
                                    MapBuilder.<String, Object>newMapBuilder().put(currentFieldName, parser.mapOrdered()).map();
                            builder.putMapping(new MappingMetadata(currentFieldName, mappingSource));
                        } else {
                            throw new IllegalArgumentException("Unexpected token: " + token);
                        }
                    }
                } else if (KEY_ALIASES.equals(currentFieldName)) {
                    while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                        builder.putAlias(AliasMetadata.Builder.fromXContent(parser));
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
                            builder.putInSyncAllocationIds(Integer.parseInt(currentFieldName), allocationIds);
                        } else {
                            throw new IllegalArgumentException("Unexpected token: " + token);
                        }
                    }
                } else if (KEY_PRIMARY_TERMS.equals(currentFieldName)) {
                    parser.skipChildren();
                } else if (KEY_ROLLOVER_INFOS.equals(currentFieldName)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            currentFieldName = parser.currentName();
                        } else if (token == XContentParser.Token.START_OBJECT) {
                            builder.putRolloverInfo(RolloverInfo.parse(parser, currentFieldName));
                        } else {
                            throw new IllegalArgumentException("Unexpected token: " + token);
                        }
                    }
                } else if ("warmers".equals(currentFieldName)) {
                    // TODO: do this in 6.0:
                    // throw new IllegalArgumentException("Warmers are not supported anymore - are you upgrading from 1.x?");
                    // ignore: warmers have been removed in 5.0 and are
                    // simply ignored when upgrading from 2.x
                    assert Version.CURRENT.major <= 5;
                    parser.skipChildren();
                } else {
                    throw new IllegalArgumentException("Unexpected field for an object " + currentFieldName);
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (KEY_MAPPINGS.equals(currentFieldName)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        if (token == XContentParser.Token.VALUE_EMBEDDED_OBJECT) {
                            builder.putMapping(new MappingMetadata(new CompressedXContent(parser.binaryValue())));
                        } else {
                            Map<String, Object> mapping = parser.mapOrdered();
                            if (mapping.size() == 1) {
                                String mappingType = mapping.keySet().iterator().next();
                                builder.putMapping(new MappingMetadata(mappingType, mapping));
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
                } else if (KEY_ALIASES.equals(currentFieldName)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        builder.putAlias(new AliasMetadata.Builder(parser.text()).build());
                    }
                } else {
                    throw new IllegalArgumentException("Unexpected field for an array " + currentFieldName);
                }
            } else if (token.isValue()) {
                if (KEY_STATE.equals(currentFieldName)) {
                    builder.state(IndexMetadata.State.fromString(parser.text()));
                } else if (KEY_VERSION.equals(currentFieldName)) {
                    builder.version(parser.longValue());
                } else if (KEY_MAPPING_VERSION.equals(currentFieldName)) {
                    mappingVersion = true;
                    builder.mappingVersion(parser.longValue());
                } else if (KEY_SETTINGS_VERSION.equals(currentFieldName)) {
                    settingsVersion = true;
                    builder.settingsVersion(parser.longValue());
                } else if (KEY_ALIASES_VERSION.equals(currentFieldName)) {
                    aliasesVersion = true;
                    builder.aliasesVersion(parser.longValue());
                } else if (KEY_ROUTING_NUM_SHARDS.equals(currentFieldName)) {
                    builder.setRoutingNumShards(parser.intValue());
                } else if (KEY_SYSTEM.equals(currentFieldName)) {
                    builder.system(parser.booleanValue());
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
