package org.xbib.elx.http.util;

import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

/**
 * Reads an object from a parser using some context.
 */
@FunctionalInterface
public interface ContextParser<Context, T> {
    T parse(XContentParser p, Context c) throws IOException;
}
