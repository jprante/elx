package org.xbib.elx.http.util.aggregations;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.xbib.elx.http.util.ObjectParser;

import java.io.IOException;
import java.nio.CharBuffer;
import java.util.List;

public class ParsedStringTerms extends ParsedTerms {

    public String getType() {
        return "terms";
    }

    private static ObjectParser<ParsedStringTerms, Void> PARSER =
            new ObjectParser<>(ParsedStringTerms.class.getSimpleName(), true, ParsedStringTerms::new);

    static {
        declareParsedTermsFields(PARSER, ParsedBucket::fromXContent);
    }

    public static ParsedStringTerms fromXContent(XContentParser parser, String name) throws IOException {
        ParsedStringTerms aggregation = PARSER.parse(parser, null);
        aggregation.setName(name);
        return aggregation;
    }

    @Override
    public Object getProperty(String path) {
        throw new UnsupportedOperationException();
    }

    public static class ParsedBucket extends ParsedTerms.ParsedBucket {

        private BytesRef key;

        @Override
        public Object getKey() {
            return getKeyAsString();
        }

        @Override
        public String getKeyAsString() {
            String keyAsString = super.getKeyAsString();
            if (keyAsString != null) {
                return keyAsString;
            }
            if (key != null) {
                return key.utf8ToString();
            }
            return null;
        }

        @Override
        public Object getProperty(String containingAggName, List<String> path) {
            throw new UnsupportedOperationException();
        }

        public Number getKeyAsNumber() {
            if (key != null) {
                return Double.parseDouble(key.utf8ToString());
            }
            return null;
        }

        protected XContentBuilder keyToXContent(XContentBuilder builder) throws IOException {
            return builder.field(CommonFields.KEY.getPreferredName(), getKey());
        }

        static ParsedBucket fromXContent(XContentParser parser) throws IOException {
            return parseTermsBucketXContent(parser, ParsedBucket::new, (p, bucket) -> {
                    CharBuffer cb = charBufferOrNull(p);
                    if (cb == null) {
                        bucket.key = null;
                    } else {
                        bucket.key = new BytesRef(cb);
                    }
                });
        }

        static CharBuffer charBufferOrNull(XContentParser parser) throws IOException {
            if (parser.currentToken() == XContentParser.Token.VALUE_NULL) {
                return null;
            }
            return CharBuffer.wrap(parser.textCharacters(), parser.textOffset(), parser.textLength());
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            throw new UnsupportedOperationException();
        }
    }
}
