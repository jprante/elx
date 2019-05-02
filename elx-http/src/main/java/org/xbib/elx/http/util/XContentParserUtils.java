package org.xbib.elx.http.util;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentLocation;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.Aggregation;
import org.xbib.elx.http.util.aggregations.ParsedStringTerms;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class XContentParserUtils {

    private static final NamedXContentRegistry xContentRegistry = new NamedXContentRegistry(getDefaultNamedXContents());

    public static void ensureExpectedToken(XContentParser.Token expected, XContentParser.Token actual, Supplier location) {
        if (actual != expected) {
            String message = "Failed to parse object: expecting token of type [%s] but found [%s]";
            throw new ElasticsearchException(location.get() + ":" + String.format(Locale.ROOT, message, expected, actual));
        }
    }

    public static <T> void parseTypedKeysObject(XContentParser parser, String delimiter, Class<T> objectClass, Consumer<T> consumer)
            throws IOException {
        if (parser.currentToken() != XContentParser.Token.START_OBJECT && parser.currentToken() != XContentParser.Token.START_ARRAY) {
            throwUnknownToken(parser.currentToken(), parser.getTokenLocation());
        }
        String currentFieldName = parser.currentName();
        if (Strings.hasLength(currentFieldName)) {
            int position = currentFieldName.indexOf(delimiter);
            if (position > 0) {
                String type = currentFieldName.substring(0, position);
                String name = currentFieldName.substring(position + 1);
                consumer.accept(namedObject(parser, objectClass, type, name));
                return;
            }
            // if we didn't find a delimiter we ignore the object or array for forward compatibility instead of throwing an error
            parser.skipChildren();
        } else {
            throw new ElasticsearchException(parser.getTokenLocation() + ":" + "Failed to parse object: empty key");
        }
    }

    public static void throwUnknownToken(XContentParser.Token token, XContentLocation location) {
        String message = "Failed to parse object: unexpected token [%s] found";
        throw new ElasticsearchException(location + ":" + String.format(Locale.ROOT, message, token));
    }

    static <T> T namedObject(XContentParser parser, Class<T> categoryClass, String name, Object context) throws IOException {
        return xContentRegistry.parseNamedObject(categoryClass, name, parser, context);
    }

    public static List<NamedXContentRegistry.Entry> getDefaultNamedXContents() {
        Map<String, ContextParser<Object, ? extends Aggregation>> map = new HashMap<>();
        //map.put("terms", (p, c) -> ParsedStringTerms.fromXContent(p, (String) c));
        return map.entrySet().stream()
                .map(entry -> new NamedXContentRegistry.Entry(Aggregation.class, new ParseField(entry.getKey()), entry.getValue()))
                .collect(Collectors.toList());
    }
}
