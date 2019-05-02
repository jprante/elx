package org.xbib.elx.http.util.aggregations;

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.search.aggregations.Aggregation;
import org.xbib.elx.http.util.ObjectParser;

import java.util.Collections;
import java.util.Map;

/**
 * An implementation of {@link Aggregation} that is parsed from a REST response.
 * Serves as a base class for all aggregation implementations that are parsed from REST.
 */
public abstract class ParsedAggregation implements Aggregation {

    protected static void declareAggregationFields(ObjectParser<? extends ParsedAggregation, Void> objectParser) {
        objectParser.declareObject((parsedAgg, metadata) -> parsedAgg.metadata = Collections.unmodifiableMap(metadata),
                (parser, context) -> parser.map(), CommonFields.META);
    }

    private String name;
    protected Map<String, Object> metadata;

    @Override
    public final String getName() {
        return name;
    }

    protected void setName(String name) {
        this.name = name;
    }

    @Override
    public final Map<String, Object> getMetaData() {
        return metadata;
    }
}