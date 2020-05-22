package org.xbib.elx.http.util;

import org.elasticsearch.common.xcontent.XContentLocation;

public class NamedObjectNotFoundException extends XContentParseException {

    public NamedObjectNotFoundException(String message) {
        this(null, message);
    }

    public NamedObjectNotFoundException(XContentLocation location, String message) {
        super(location, message);
    }
}
