package org.xbib.elx.http.util.aggregations;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.xbib.elx.http.util.CheckedBiConsumer;
import org.xbib.elx.http.util.CheckedFunction;
import org.xbib.elx.http.util.ObjectParser;
import org.xbib.elx.http.util.XContentParserUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

public abstract class ParsedTerms extends ParsedMultiBucketAggregation<ParsedTerms.ParsedBucket> implements Terms {

    protected static final ParseField DOC_COUNT_ERROR_UPPER_BOUND_FIELD_NAME = new ParseField("doc_count_error_upper_bound");

    protected static final ParseField SUM_OF_OTHER_DOC_COUNTS = new ParseField("sum_other_doc_count");

    protected long docCountErrorUpperBound;

    protected long sumOtherDocCount;

    @Override
    public long getDocCountError() {
        return docCountErrorUpperBound;
    }

    @Override
    public long getSumOfOtherDocCounts() {
        return sumOtherDocCount;
    }

    @Override
    public List<Terms.Bucket> getBuckets() {
        //return buckets;
        throw new UnsupportedOperationException();
    }

    @Override
    public Terms.Bucket getBucketByKey(String term) {
        for (Terms.Bucket bucket : getBuckets()) {
            if (bucket.getKeyAsString().equals(term)) {
                return bucket;
            }
        }
        return null;
    }

    static void declareParsedTermsFields(final ObjectParser<? extends ParsedTerms, Void> objectParser,
                                         final CheckedFunction<XContentParser, ParsedBucket, IOException> bucketParser) {
        declareMultiBucketAggregationFields(objectParser, bucketParser::apply, bucketParser::apply);
        objectParser.declareLong((parsedTerms, value) -> parsedTerms.docCountErrorUpperBound = value ,
                DOC_COUNT_ERROR_UPPER_BOUND_FIELD_NAME);
        objectParser.declareLong((parsedTerms, value) -> parsedTerms.sumOtherDocCount = value,
                SUM_OF_OTHER_DOC_COUNTS);
    }

    public abstract static class ParsedBucket extends ParsedMultiBucketAggregation.ParsedBucket /*implements Terms.Bucket*/ {

        boolean showDocCountError = false;
        protected long docCountError;

        public long getDocCountError() {
            return docCountError;
        }

        @Override
        public final XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            /*builder.startObject();
            keyToXContent(builder);
            builder.field(CommonFields.DOC_COUNT.getPreferredName(), getDocCount());
            if (showDocCountError) {
                builder.field(DOC_COUNT_ERROR_UPPER_BOUND_FIELD_NAME.getPreferredName(), getDocCountError());
            }
            getAggregations().toXContentInternal(builder, params);
            builder.endObject();*/
            return builder;
        }

        static <B extends ParsedBucket> B parseTermsBucketXContent(final XContentParser parser, final Supplier<B> bucketSupplier,
                                                                   final CheckedBiConsumer<XContentParser, B, IOException> keyConsumer)
                throws IOException {

            final B bucket = bucketSupplier.get();
            final List<InternalAggregation> aggregations = new ArrayList<>();
            XContentParser.Token token;
            String currentFieldName = parser.currentName();
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
                    } else if (DOC_COUNT_ERROR_UPPER_BOUND_FIELD_NAME.getPreferredName().equals(currentFieldName)) {
                        bucket.docCountError = parser.longValue();
                        bucket.showDocCountError = true;
                    }
                } else if (token == XContentParser.Token.START_OBJECT) {
                    XContentParserUtils.parseTypedKeysObject(parser, "#", InternalAggregation.class,
                            aggregations::add);
                }
            }
            bucket.setAggregations(new InternalAggregations(aggregations));
            return bucket;
        }
    }
}
