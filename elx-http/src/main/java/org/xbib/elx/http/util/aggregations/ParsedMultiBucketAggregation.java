package org.xbib.elx.http.util.aggregations;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.xbib.elx.http.util.CheckedBiConsumer;
import org.xbib.elx.http.util.CheckedFunction;
import org.xbib.elx.http.util.ObjectParser;
import org.xbib.elx.http.util.XContentParserUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.xbib.elx.http.util.XContentParserUtils.ensureExpectedToken;

public abstract class ParsedMultiBucketAggregation<B extends ParsedMultiBucketAggregation.Bucket>
        extends ParsedAggregation implements MultiBucketsAggregation {

    protected final List<B> buckets = new ArrayList<>();

    protected boolean keyed = false;

    protected static void declareMultiBucketAggregationFields(final ObjectParser<? extends ParsedMultiBucketAggregation, Void> objectParser,
                                                  final CheckedFunction<XContentParser, ParsedBucket, IOException> bucketParser,
                                                  final CheckedFunction<XContentParser, ParsedBucket, IOException> keyedBucketParser) {
        declareAggregationFields(objectParser);
        objectParser.declareField((parser, aggregation, context) -> {
            XContentParser.Token token = parser.currentToken();
            if (token == XContentParser.Token.START_OBJECT) {
                aggregation.keyed = true;
                while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                    aggregation.buckets.add(keyedBucketParser.apply(parser));
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                aggregation.keyed = false;
                while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                    aggregation.buckets.add(bucketParser.apply(parser));
                }
            }
        }, CommonFields.BUCKETS, ObjectParser.ValueType.OBJECT_ARRAY);
    }

    public abstract static class ParsedBucket implements MultiBucketsAggregation.Bucket {

        private Aggregations aggregations;
        private String keyAsString;
        private long docCount;
        private boolean keyed;

        protected void setKeyAsString(String keyAsString) {
            this.keyAsString = keyAsString;
        }

        @Override
        public String getKeyAsString() {
            return keyAsString;
        }

        protected void setDocCount(long docCount) {
            this.docCount = docCount;
        }

        @Override
        public long getDocCount() {
            return docCount;
        }

        public void setKeyed(boolean keyed) {
            this.keyed = keyed;
        }

        protected boolean isKeyed() {
            return keyed;
        }

        protected void setAggregations(Aggregations aggregations) {
            this.aggregations = aggregations;
        }

        @Override
        public Aggregations getAggregations() {
            return aggregations;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            /*if (keyed) {
                builder.startObject(getKeyAsString());
            } else {
                builder.startObject();
            }
            if (keyAsString != null) {
                builder.field(CommonFields.KEY_AS_STRING.getPreferredName(), getKeyAsString());
            }
            keyToXContent(builder);
            builder.field(CommonFields.DOC_COUNT.getPreferredName(), docCount);
            aggregations.toXContentInternal(builder, params);
            builder.endObject();*/
            return builder;
        }

        protected XContentBuilder keyToXContent(XContentBuilder builder) throws IOException {
            return builder.field(CommonFields.KEY.getPreferredName(), getKey());
        }

        protected static <B extends ParsedBucket> B parseXContent(final XContentParser parser,
                                                                  final boolean keyed,
                                                                  final Supplier<B> bucketSupplier,
                                                                  final CheckedBiConsumer<XContentParser, B, IOException> keyConsumer)
                                                                        throws IOException {
            final B bucket = bucketSupplier.get();
            bucket.setKeyed(keyed);
            XContentParser.Token token = parser.currentToken();
            String currentFieldName = parser.currentName();
            if (keyed) {
                ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser::getTokenLocation);
                ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
            }
            List<InternalAggregation> aggregations = new ArrayList<>();
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token.isValue()) {
                    if (CommonFields.KEY_AS_STRING.getPreferredName().equals(currentFieldName)) {
                        bucket.setKeyAsString(parser.text());
                    } else if (CommonFields.KEY.getPreferredName().equals(currentFieldName)) {
                        keyConsumer.accept(parser, bucket);
                    } else if (CommonFields.DOC_COUNT.getPreferredName().equals(currentFieldName)) {
                        bucket.setDocCount(parser.longValue());
                    }
                } else if (token == XContentParser.Token.START_OBJECT) {
                    if (CommonFields.KEY.getPreferredName().equals(currentFieldName)) {
                        keyConsumer.accept(parser, bucket);
                    } else {
                        XContentParserUtils.parseTypedKeysObject(parser, "#", InternalAggregation.class,
                            aggregations::add);
                    }
                }
            }
            bucket.setAggregations(new InternalAggregations(aggregations));
            return bucket;
        }
    }
}
