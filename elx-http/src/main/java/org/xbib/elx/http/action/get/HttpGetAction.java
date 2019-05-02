package org.xbib.elx.http.action.get;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.GenericAction;
import org.elasticsearch.action.get.GetAction;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentLocation;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.get.GetField;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.mapper.internal.SourceFieldMapper;
import org.xbib.elx.http.util.CheckedFunction;
import org.xbib.elx.http.HttpAction;
import org.xbib.netty.http.client.RequestBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Supplier;

public class HttpGetAction extends HttpAction<GetRequest, GetResponse> {

    @Override
    public GenericAction<GetRequest, GetResponse> getActionInstance() {
        return GetAction.INSTANCE;
    }

    @Override
    protected RequestBuilder createHttpRequest(String url, GetRequest request) {
        return newGetRequest(url, request.index() + "/" + request.type() + "/" + request.id());
    }

    @Override
    protected CheckedFunction<XContentParser, GetResponse, IOException> entityParser() {
        return this::fromXContent;
    }

    public GetResponse fromXContent(XContentParser parser) throws IOException {
        GetResult getResult = Helper.fromXContent(parser);
        if (getResult.getIndex() == null && getResult.getType() == null && getResult.getId() == null) {
            throw new ElasticsearchException(parser.getTokenLocation() + ":" +
                    String.format(Locale.ROOT, "Missing required fields [%s,%s,%s]", "_index", "_type", "_id"));
        }
        return new GetResponse(getResult);
    }

    static class Helper {

        private static final Logger logger = LogManager.getLogger("helper");

        static final String _INDEX = "_index";
        static final String _TYPE = "_type";
        static final String _ID = "_id";
        private static final String _VERSION = "_version";
        private static final String FOUND = "found";
        private static final String FIELDS = "fields";

        static void ensureExpectedToken(XContentParser.Token expected, XContentParser.Token actual, Supplier location) {
            if (actual != expected) {
                String message = "Failed to parse object: expecting token of type [%s] but found [%s]";
                throw new ElasticsearchException(location.get() + ":" + String.format(Locale.ROOT, message, expected, actual));
            }
        }

        static GetResult fromXContent(XContentParser parser) throws IOException {
            XContentParser.Token token = parser.nextToken();
            ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser::getTokenLocation);
            return fromXContentEmbedded(parser);
        }

        static GetResult fromXContentEmbedded(XContentParser parser) throws IOException {
            XContentParser.Token token = parser.nextToken();
            ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser::getTokenLocation);
            return fromXContentEmbedded(parser, null, null, null);
        }

        static GetResult fromXContentEmbedded(XContentParser parser, String index, String type, String id) throws IOException {
            XContentParser.Token token = parser.currentToken();
            ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser::getTokenLocation);
            String currentFieldName = parser.currentName();
            long version = -1;
            Boolean found = null;
            BytesReference source = null;
            Map<String, GetField> fields = new HashMap<>();
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token.isValue()) {
                    if (_INDEX.equals(currentFieldName)) {
                        index = parser.text();
                    } else if (_TYPE.equals(currentFieldName)) {
                        type = parser.text();
                    } else if (_ID.equals(currentFieldName)) {
                        id = parser.text();
                    }  else if (_VERSION.equals(currentFieldName)) {
                        version = parser.longValue();
                    } else if (FOUND.equals(currentFieldName)) {
                        found = parser.booleanValue();
                    } else {
                        fields.put(currentFieldName, new GetField(currentFieldName, Collections.singletonList(parser.objectText())));
                    }
                } else if (token == XContentParser.Token.START_OBJECT) {
                    if (SourceFieldMapper.NAME.equals(currentFieldName)) {
                        try (XContentBuilder builder = XContentBuilder.builder(parser.contentType().xContent())) {
                            builder.copyCurrentStructure(parser);
                            source = builder.bytes();
                        }
                    } else if (FIELDS.equals(currentFieldName)) {
                        while(parser.nextToken() != XContentParser.Token.END_OBJECT) {
                            GetField getField = getFieldFromXContent(parser);
                            fields.put(getField.getName(), getField);
                        }
                    } else {
                        parser.skipChildren();
                    }
                } else if (token == XContentParser.Token.START_ARRAY) {
                    if ("_ignored".equals(currentFieldName)) {
                        fields.put(currentFieldName, new GetField(currentFieldName, parser.list()));
                    } else {
                        parser.skipChildren();
                    }
                }
            }
            return new GetResult(index, type, id, version, found, source, fields);
        }

        static GetField getFieldFromXContent(XContentParser parser) throws IOException {
            ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.currentToken(), parser::getTokenLocation);
            String fieldName = parser.currentName();
            XContentParser.Token token = parser.nextToken();
            ensureExpectedToken(XContentParser.Token.START_ARRAY, token, parser::getTokenLocation);
            List<Object> values = new ArrayList<>();
            while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                values.add(parseFieldsValue(parser));
            }
            return new GetField(fieldName, values);
        }

        static Object parseFieldsValue(XContentParser parser) throws IOException {
            XContentParser.Token token = parser.currentToken();
            Object value = null;
            if (token == XContentParser.Token.VALUE_STRING) {
                //binary values will be parsed back and returned as base64 strings when reading from json and yaml
                value = parser.text();
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                value = parser.numberValue();
            } else if (token == XContentParser.Token.VALUE_BOOLEAN) {
                value = parser.booleanValue();
            } else if (token == XContentParser.Token.VALUE_EMBEDDED_OBJECT) {
                //binary values will be parsed back and returned as BytesArray when reading from cbor and smile
                value = new BytesArray(parser.binaryValue());
            } else if (token == XContentParser.Token.VALUE_NULL) {
                value = null;
            } else if (token == XContentParser.Token.START_OBJECT) {
                value = parser.mapOrdered();
            } else if (token == XContentParser.Token.START_ARRAY) {
                value = parser.listOrderedMap();
            } else {
                throwUnknownToken(token, parser.getTokenLocation());
            }
            return value;
        }

        static void throwUnknownToken(XContentParser.Token token, XContentLocation location) {
            String message = "Failed to parse object: unexpected token [%s] found";
            throw new ElasticsearchException(location + ":" + String.format(Locale.ROOT, message, token));
        }
    }
}
