package org.xbib.elx.http.util.aggregations;

import org.elasticsearch.common.ParseField;

final class CommonFields {
    public static final ParseField META = new ParseField("meta");
    public static final ParseField BUCKETS = new ParseField("buckets");
    public static final ParseField VALUE = new ParseField("value");
    public static final ParseField VALUES = new ParseField("values");
    public static final ParseField VALUE_AS_STRING = new ParseField("value_as_string");
    public static final ParseField DOC_COUNT = new ParseField("doc_count");
    public static final ParseField KEY = new ParseField("key");
    public static final ParseField KEY_AS_STRING = new ParseField("key_as_string");
    public static final ParseField FROM = new ParseField("from");
    public static final ParseField FROM_AS_STRING = new ParseField("from_as_string");
    public static final ParseField TO = new ParseField("to");
    public static final ParseField TO_AS_STRING = new ParseField("to_as_string");
}
