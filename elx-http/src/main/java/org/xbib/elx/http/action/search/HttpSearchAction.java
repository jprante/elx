package org.xbib.elx.http.action.search;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.internal.SourceFieldMapper;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.elasticsearch.search.internal.InternalSearchHits;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.Suggest.Suggestion.Entry;
import org.elasticsearch.search.suggest.Suggest.Suggestion.Entry.Option;
import org.xbib.elx.http.util.CheckedFunction;
import org.xbib.elx.http.HttpAction;
import org.xbib.elx.http.util.ObjectParser;
import org.xbib.elx.http.util.XContentParserUtils;
import org.xbib.netty.http.client.RequestBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.xbib.elx.http.util.ObjectParser.ValueType.STRING;
import static org.xbib.elx.http.util.XContentParserUtils.ensureExpectedToken;

public class HttpSearchAction extends HttpAction<SearchRequest, SearchResponse> {

    @Override
    public SearchAction getActionInstance() {
        return SearchAction.INSTANCE;
    }

    @Override
    protected RequestBuilder createHttpRequest(String url, SearchRequest request) {
        String index = request.indices() != null ? "/" + String.join(",", request.indices()) : "";
        return newPostRequest(url, index + "/_search", request.source());
    }

    @Override
    protected CheckedFunction<XContentParser, SearchResponse, IOException> entityParser() {
        return Helper::fromXContent;
    }

    public static class Helper {

        private static final Logger logger = LogManager.getLogger("helper");

        private static final ParseField SCROLL_ID = new ParseField("_scroll_id");
        private static final ParseField TOOK = new ParseField("took");
        private static final ParseField TIMED_OUT = new ParseField("timed_out");
        private static final ParseField TERMINATED_EARLY = new ParseField("terminated_early");

        private static final ParseField _SHARDS_FIELD = new ParseField("_shards");
        private static final ParseField TOTAL_FIELD = new ParseField("total");
        private static final ParseField SUCCESSFUL_FIELD = new ParseField("successful");
        private static final ParseField SKIPPED_FIELD = new ParseField("skipped");
        private static final ParseField FAILED_FIELD = new ParseField("failed");
        private static final ParseField FAILURES_FIELD = new ParseField("failures");

        private static final String HITS = "hits";

        private static final String TOTAL = "total";
        private static final String MAX_SCORE = "max_score";

        private static final String _NESTED = "_nested";

        private static final String _INDEX = "_index";
        private static final String _TYPE = "_type";
        private static final String _ID = "_id";
        private static final String _VERSION = "_version";
        private static final String _SCORE = "_score";
        private static final String FIELDS = "fields";
        private static final String HIGHLIGHT = "highlight";
        private static final String SORT = "sort";
        private static final String MATCHED_QUERIES = "matched_queries";
        private static final String _EXPLANATION = "_explanation";
        private static final String INNER_HITS = "inner_hits";
        private static final String _SHARD = "_shard";
        private static final String _NODE = "_node";

        private static final String AGGREGATIONS_FIELD = "aggregations";

        private static final String TYPED_KEYS_DELIMITER = "#";

        private static final String SUGGEST_NAME = "suggest";

        private static final String REASON_FIELD = "reason";
        private static final String NODE_FIELD = "node";
        private static final String INDEX_FIELD = "index";
        private static final String SHARD_FIELD = "shard";

        private static final String TYPE = "type";
        private static final String REASON = "reason";
        private static final String CAUSED_BY = "caused_by";
        private static final String STACK_TRACE = "stack_trace";
        private static final String HEADER = "header";
        private static final String ROOT_CAUSE = "root_cause";

        private static ObjectParser<Map<String, Object>, Void> MAP_PARSER =
                new ObjectParser<>("innerHitParser", true, HashMap::new);


        static {
            declareInnerHitsParseFields(MAP_PARSER);
        }

        public static SearchResponse fromXContent(XContentParser parser) throws IOException {
            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
            parser.nextToken();
            return innerFromXContent(parser);
        }

        static SearchResponse innerFromXContent(XContentParser parser) throws IOException {
            ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.currentToken(), parser::getTokenLocation);
            String currentFieldName = parser.currentName();
            InternalSearchHits hits = null;
            InternalAggregations aggs = null;
            Suggest suggest = null;
            boolean timedOut = false;
            Boolean terminatedEarly = null;
            long tookInMillis = -1;
            int successfulShards = -1;
            int totalShards = -1;
            String scrollId = null;
            List<ShardSearchFailure> failures = new ArrayList<>();
            ParseFieldMatcher matcher = new ParseFieldMatcher(Settings.EMPTY);
            for (XContentParser.Token token = parser.nextToken(); token != XContentParser.Token.END_OBJECT; token = parser.nextToken()) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token.isValue()) {
                    if (matcher.match(currentFieldName, SCROLL_ID)) {
                        scrollId = parser.text();
                    } else if (matcher.match(currentFieldName, TOOK)) {
                        tookInMillis = parser.longValue();
                    } else if (matcher.match(currentFieldName, TIMED_OUT)) {
                        timedOut = parser.booleanValue();
                    } else if (matcher.match(currentFieldName, TERMINATED_EARLY)) {
                        terminatedEarly = parser.booleanValue();
                    } else {
                        parser.skipChildren();
                    }
                } else if (token == XContentParser.Token.START_OBJECT) {
                    if (HITS.equals(currentFieldName)) {
                        logger.debug("searchHitsFromXContent");
                        hits = searchHitsFromXContent(parser);
                    } else if (AGGREGATIONS_FIELD.equals(currentFieldName)) {
                        aggs = aggregationsFromXContent(parser);
                    } else if (SUGGEST_NAME.equals(currentFieldName)) {
                        suggest = suggestFromXContent(parser);
                    } else if (matcher.match(currentFieldName, _SHARDS_FIELD)) {
                        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                            if (token == XContentParser.Token.FIELD_NAME) {
                                currentFieldName = parser.currentName();
                            } else if (token.isValue()) {
                                if (matcher.match(currentFieldName, FAILED_FIELD)) {
                                    parser.intValue(); // we don't need it but need to consume it
                                } else if (matcher.match(currentFieldName, SUCCESSFUL_FIELD)) {
                                    successfulShards = parser.intValue();
                                } else if (matcher.match(currentFieldName, TOTAL_FIELD)) {
                                    totalShards = parser.intValue();
                                } else {
                                    parser.skipChildren();
                                }
                            } else if (token == XContentParser.Token.START_ARRAY) {
                                if (matcher.match(currentFieldName, FAILURES_FIELD)) {
                                    while((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                                        failures.add(shardSearchFailureFromXContent(parser));
                                    }
                                } else {
                                    parser.skipChildren();
                                }
                            } else {
                                parser.skipChildren();
                            }
                        }
                    } else {
                        parser.skipChildren();
                    }
                }
            }
            // TODO profileResults
            InternalSearchResponse internalResponse = new InternalSearchResponse(hits, aggs, suggest,
                    null, timedOut, terminatedEarly);
            return new SearchResponse(internalResponse, scrollId, totalShards, successfulShards, tookInMillis,
                    failures.toArray(ShardSearchFailure.EMPTY_ARRAY));
        }

        static InternalSearchHits searchHitsFromXContent(XContentParser parser) throws IOException {
            if (parser.currentToken() != XContentParser.Token.START_OBJECT) {
                parser.nextToken();
                ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser::getTokenLocation);
            }
            XContentParser.Token token = parser.currentToken();
            String currentFieldName = null;
            List<InternalSearchHit> hits = new ArrayList<>();
            long totalHits = -1L;
            float maxScore = 0f;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token.isValue()) {
                    if (TOTAL.equals(currentFieldName)) {
                        totalHits = parser.longValue();
                    } else if (MAX_SCORE.equals(currentFieldName)) {
                        maxScore = parser.floatValue();
                    }
                } else if (token == XContentParser.Token.VALUE_NULL) {
                    if (MAX_SCORE.equals(currentFieldName)) {
                        maxScore = Float.NaN; // NaN gets rendered as null-field
                    }
                } else if (token == XContentParser.Token.START_ARRAY) {
                    if (HITS.equals(currentFieldName)) {
                        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                            logger.debug("searchHitFromXContent");
                            hits.add(searchHitFromXContent(parser));
                        }
                    } else {
                        parser.skipChildren();
                    }
                }
            }
            InternalSearchHit[] internalSearchHits = hits.toArray(new InternalSearchHit[0]);
            return new InternalSearchHits(internalSearchHits, totalHits, maxScore);
        }

        static InternalSearchHit searchHitFromXContent(XContentParser parser) {
            return createFromMap(MAP_PARSER.apply(parser, null));
        }

        static InternalSearchHit createFromMap(Map<String, Object> values) {
            logger.debug("values = {}", values);
            String id = get(_ID, values, null);
            Text type = get(_TYPE, values, null);
            InternalSearchHit.InternalNestedIdentity nestedIdentity = get(_NESTED, values, null);
            Map<String, SearchHitField> fields = get(FIELDS, values, Collections.emptyMap());
            InternalSearchHit searchHit = new InternalSearchHit(-1, id, type, nestedIdentity, fields);
            String index = get(_INDEX, values, null);
            ShardId shardId = get(_SHARD, values, null);
            String nodeId = get(_NODE, values, null);
            if (shardId != null && nodeId != null) {
                assert shardId.index().getName().equals(index);
                searchHit.shard(new SearchShardTarget(nodeId, index, shardId.id()));
            }
            searchHit.score(get(_SCORE, values, Float.NaN));
            searchHit.version(get(_VERSION, values, -1L));
            searchHit.sortValues(get(SORT, values, new Object[0]));
            searchHit.highlightFields(get(HIGHLIGHT, values, null));
            searchHit.sourceRef(get(SourceFieldMapper.NAME, values, null));
            searchHit.explanation(get(_EXPLANATION, values, null));
            searchHit.setInnerHits(get(INNER_HITS, values, null));
            List<String> matchedQueries = get(MATCHED_QUERIES, values, null);
            if (matchedQueries != null) {
                searchHit.matchedQueries(matchedQueries.toArray(new String[0]));
            }
            return searchHit;
        }

        @SuppressWarnings("unchecked")
        private static <T> T get(String key, Map<String, Object> map, T defaultValue) {
            return (T) map.getOrDefault(key, defaultValue);
        }

        static InternalAggregations aggregationsFromXContent(XContentParser parser) throws IOException {
            final List<InternalAggregation> aggregations = new ArrayList<>();
            XContentParser.Token token;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.START_OBJECT) {
                    SetOnce<InternalAggregation> typedAgg = new SetOnce<>();
                    String currentField = parser.currentName();
                    XContentParserUtils.parseTypedKeysObject(parser, TYPED_KEYS_DELIMITER, InternalAggregation.class, typedAgg::set);
                    if (typedAgg.get() != null) {
                        aggregations.add(typedAgg.get());
                    } else {
                        throw new ElasticsearchException(parser.getTokenLocation() + ":" +
                                String.format(Locale.ROOT, "Could not parse aggregation keyed as [%s]", currentField));
                    }
                }
            }
            return new InternalAggregations(aggregations);
        }

        static Suggest suggestFromXContent(XContentParser parser) throws IOException {
            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser::getTokenLocation);
            List<Suggest.Suggestion<? extends Entry<? extends Option>>> suggestions = new ArrayList<>();
            while ((parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.currentToken(), parser::getTokenLocation);
                String currentField = parser.currentName();
                ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.nextToken(), parser::getTokenLocation);
                Suggest.Suggestion<? extends Entry<? extends Option>> suggestion = suggestionFromXContent(parser);
                if (suggestion != null) {
                    suggestions.add(suggestion);
                } else {
                    throw new ElasticsearchException(parser.getTokenLocation() + ":" +
                            String.format(Locale.ROOT, "Could not parse suggestion keyed as [%s]", currentField));
                }
            }
            return new Suggest(suggestions);
        }

        static Suggest.Suggestion<? extends Entry<? extends Option>> suggestionFromXContent(XContentParser parser) throws IOException {
            ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.currentToken(), parser::getTokenLocation);
            SetOnce<Suggest.Suggestion> suggestion = new SetOnce<>();
            XContentParserUtils.parseTypedKeysObject(parser, "#", Suggest.Suggestion.class, suggestion::set);
            return suggestion.get();
        }

        static ShardSearchFailure shardSearchFailureFromXContent(XContentParser parser) throws IOException {
            XContentParser.Token token;
            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser::getTokenLocation);
            String currentFieldName = null;
            int shardId = -1;
            String indexName = null;
            String nodeId = null;
            ElasticsearchException exception = null;
            while((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token.isValue()) {
                    if (SHARD_FIELD.equals(currentFieldName)) {
                        shardId  = parser.intValue();
                    } else if (INDEX_FIELD.equals(currentFieldName)) {
                        indexName  = parser.text();
                    } else if (NODE_FIELD.equals(currentFieldName)) {
                        nodeId  = parser.text();
                    } else {
                        parser.skipChildren();
                    }
                } else if (token == XContentParser.Token.START_OBJECT) {
                    if (REASON_FIELD.equals(currentFieldName)) {
                        exception = elasticsearchExceptionFromXContent(parser);
                    } else {
                        parser.skipChildren();
                    }
                } else {
                    parser.skipChildren();
                }
            }
            SearchShardTarget searchShardTarget = null;
            if (nodeId != null) {
                searchShardTarget = new SearchShardTarget(nodeId, indexName, shardId);
            }
            return new ShardSearchFailure(exception, searchShardTarget);
        }

        public static ElasticsearchException elasticsearchExceptionFromXContent(XContentParser parser) throws IOException {
            XContentParser.Token token = parser.nextToken();
            ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser::getTokenLocation);
            return elasticsearchExceptionFromXContent(parser, false);
        }

        static ElasticsearchException elasticsearchExceptionFromXContent(XContentParser parser, boolean parseRootCauses)
                throws IOException {
            XContentParser.Token token = parser.currentToken();
            ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser::getTokenLocation);

            String type = null, reason = null, stack = null;
            ElasticsearchException cause = null;
            Map<String, List<String>> metadata = new HashMap<>();
            Map<String, List<String>> headers = new HashMap<>();
            List<ElasticsearchException> rootCauses = new ArrayList<>();

            for (; token == XContentParser.Token.FIELD_NAME; token = parser.nextToken()) {
                String currentFieldName = parser.currentName();
                token = parser.nextToken();

                if (token.isValue()) {
                    if (TYPE.equals(currentFieldName)) {
                        type = parser.text();
                    } else if (REASON.equals(currentFieldName)) {
                        reason = parser.text();
                    } else if (STACK_TRACE.equals(currentFieldName)) {
                        stack = parser.text();
                    } else if (token == XContentParser.Token.VALUE_STRING) {
                        metadata.put(currentFieldName, Collections.singletonList(parser.text()));
                    }
                } else if (token == XContentParser.Token.START_OBJECT) {
                    if (CAUSED_BY.equals(currentFieldName)) {
                        cause = elasticsearchExceptionFromXContent(parser);
                    } else if (HEADER.equals(currentFieldName)) {
                        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                            if (token == XContentParser.Token.FIELD_NAME) {
                                currentFieldName = parser.currentName();
                            } else {
                                List<String> values = headers.getOrDefault(currentFieldName, new ArrayList<>());
                                if (token == XContentParser.Token.VALUE_STRING) {
                                    values.add(parser.text());
                                } else if (token == XContentParser.Token.START_ARRAY) {
                                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                                        if (token == XContentParser.Token.VALUE_STRING) {
                                            values.add(parser.text());
                                        } else {
                                            parser.skipChildren();
                                        }
                                    }
                                } else if (token == XContentParser.Token.START_OBJECT) {
                                    parser.skipChildren();
                                }
                                headers.put(currentFieldName, values);
                            }
                        }
                    } else {
                        parser.skipChildren();
                    }
                } else if (token == XContentParser.Token.START_ARRAY) {
                    if (parseRootCauses && ROOT_CAUSE.equals(currentFieldName)) {
                        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                            rootCauses.add(elasticsearchExceptionFromXContent(parser));
                        }
                    } else {
                        List<String> values = new ArrayList<>();
                        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                            if (token == XContentParser.Token.VALUE_STRING) {
                                values.add(parser.text());
                            } else {
                                parser.skipChildren();
                            }
                        }
                        if (values.size() > 0) {
                            if (metadata.containsKey(currentFieldName)) {
                                values.addAll(metadata.get(currentFieldName));
                            }
                            metadata.put(currentFieldName, values);
                        }
                    }
                }
            }
            ElasticsearchException e = new ElasticsearchException(buildMessage(type, reason, stack), cause);
            for (Map.Entry<String, List<String>> header : headers.entrySet()) {
                e.addHeader(header.getKey(), header.getValue());
            }
            for (ElasticsearchException rootCause : rootCauses) {
                e.addSuppressed(rootCause);
            }
            return e;
        }

        static String buildMessage(String type, String reason, String stack) {
            StringBuilder message = new StringBuilder("Elasticsearch exception [");
            message.append(TYPE).append('=').append(type).append(", ");
            message.append(REASON).append('=').append(reason);
            if (stack != null) {
                message.append(", ").append(STACK_TRACE).append('=').append(stack);
            }
            message.append(']');
            return message.toString();
        }

        private static void declareInnerHitsParseFields(ObjectParser<Map<String, Object>, Void> parser) {
            declareMetaDataFields(parser);
            parser.declareString((map, value) -> map.put(_TYPE, new Text(value)), new ParseField(_TYPE));
            parser.declareString((map, value) -> map.put(_INDEX, value), new ParseField(_INDEX));
            parser.declareString((map, value) -> map.put(_ID, value), new ParseField(_ID));
            parser.declareString((map, value) -> map.put(_NODE, value), new ParseField(_NODE));
            parser.declareField((map, value) -> map.put(_SCORE, value), SearchHit::parseScore, new ParseField(_SCORE),
                    ObjectParser.ValueType.FLOAT_OR_NULL);
            parser.declareLong((map, value) -> map.put(_VERSION, value), new ParseField(_VERSION));
            parser.declareField((map, value) -> map.put(_SHARD, value), (p, c) -> ShardId.fromString(p.text()),
                    new ParseField(_SHARD), STRING);
            parser.declareObject((map, value) -> map.put(SourceFieldMapper.NAME, value), (p, c) -> parseSourceBytes(p),
                    new ParseField(SourceFieldMapper.NAME));
            parser.declareObject((map, value) -> map.put(HIGHLIGHT, value), (p, c) -> parseHighlightFields(p),
                    new ParseField(HIGHLIGHT));
            parser.declareObject((map, value) -> {
                Map<String, SearchHitField> fieldMap = get(FIELDS, map, new HashMap<String, SearchHitField>());
                fieldMap.putAll(value);
                map.put(FIELDS, fieldMap);
            }, (p, c) -> parseFields(p), new ParseField(FIELDS));
            parser.declareObject((map, value) -> map.put(_EXPLANATION, value), (p, c) -> parseExplanation(p),
                    new ParseField(_EXPLANATION));
            parser.declareObject((map, value) -> map.put(_NESTED, value), SearchHit.NestedIdentity::fromXContent,
                    new ParseField(_NESTED));
            parser.declareObject((map, value) -> map.put(INNER_HITS, value), (p,c) -> parseInnerHits(p),
                    new ParseField(INNER_HITS));
            parser.declareStringArray((map, list) -> map.put(MATCHED_QUERIES, list), new ParseField(MATCHED_QUERIES));
            parser.declareField((map, list) -> map.put(SORT, list), SearchSortValues::fromXContent, new ParseField(SORT),
                    ObjectParser.ValueType.OBJECT_ARRAY);
        }

        private static void declareMetaDataFields(ObjectParser<Map<String, Object>, Void> parser) {
            for (String metadatafield : MapperService.getAllMetaFields()) {
                if (!metadatafield.equals(_ID) && !metadatafield.equals(_INDEX) && !metadatafield.equals(_TYPE)) {
                    parser.declareField((map, field) -> {
                        @SuppressWarnings("unchecked")
                        Map<String, SearchHitField> fieldMap = (Map<String, SearchHitField>) map.computeIfAbsent(FIELDS,
                                v -> new HashMap<String, SearchHitField>());
                        fieldMap.put(field.getName(), field);
                    }, (p, c) -> {
                        List<Object> values = new ArrayList<>();
                        values.add(parseFieldsValue(p));
                        return new InternalSearchHit(metadatafield, values);
                    }, new ParseField(metadatafield), ObjectParser.ValueType.VALUE);
                }
            }
        }

        private static Map<String, SearchHitField> parseFields(XContentParser parser) throws IOException {
            Map<String, SearchHitField> fields = new HashMap<>();
            while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                SearchHitField field = SearchHitField.fromXContent(parser);
                fields.put(field.getName(), field);
            }
            return fields;
        }

        private static Map<String, SearchHits> parseInnerHits(XContentParser parser) throws IOException {
            Map<String, SearchHits> innerHits = new HashMap<>();
            while ((parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.currentToken(), parser::getTokenLocation);
                String name = parser.currentName();
                ensureExpectedToken(Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
                ensureFieldName(parser, parser.nextToken(), SearchHits.Fields.HITS);
                innerHits.put(name, SearchHits.fromXContent(parser));
                ensureExpectedToken(XContentParser.Token.END_OBJECT, parser.nextToken(), parser::getTokenLocation);
            }
            return innerHits;
        }

        private static Map<String, HighlightField> parseHighlightFields(XContentParser parser) throws IOException {
            Map<String, HighlightField> highlightFields = new HashMap<>();
            while((parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                HighlightField highlightField = HighlightField.fromXContent(parser);
                highlightFields.put(highlightField.getName(), highlightField);
            }
            return highlightFields;
        }

        private static Explanation parseExplanation(XContentParser parser) throws IOException {
            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser::getTokenLocation);
            XContentParser.Token token;
            Float value = null;
            String description = null;
            List<Explanation> details = new ArrayList<>();
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser::getTokenLocation);
                String currentFieldName = parser.currentName();
                token = parser.nextToken();
                if (Fields.VALUE.equals(currentFieldName)) {
                    value = parser.floatValue();
                } else if (Fields.DESCRIPTION.equals(currentFieldName)) {
                    description = parser.textOrNull();
                } else if (Fields.DETAILS.equals(currentFieldName)) {
                    ensureExpectedToken(XContentParser.Token.START_ARRAY, token, parser::getTokenLocation);
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        details.add(parseExplanation(parser));
                    }
                } else {
                    parser.skipChildren();
                }
            }
            if (value == null) {
                throw new ParsingException(parser.getTokenLocation(), "missing explanation value");
            }
            if (description == null) {
                throw new ParsingException(parser.getTokenLocation(), "missing explanation description");
            }
            return Explanation.match(value, description, details);
        }

        private void buildExplanation(XContentBuilder builder, Explanation explanation) throws IOException {
            builder.startObject();
            builder.field(Fields.VALUE, explanation.getValue());
            builder.field(Fields.DESCRIPTION, explanation.getDescription());
            Explanation[] innerExps = explanation.getDetails();
            if (innerExps != null) {
                builder.startArray(Fields.DETAILS);
                for (Explanation exp : innerExps) {
                    buildExplanation(builder, exp);
                }
                builder.endArray();
            }
            builder.endObject();
        }

    }
}

