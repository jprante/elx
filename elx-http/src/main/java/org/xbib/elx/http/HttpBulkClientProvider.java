package org.xbib.elx.http;

import org.xbib.elx.api.BulkClientProvider;

public class HttpBulkClientProvider implements BulkClientProvider<HttpBulkClient> {

    @Override
    public HttpBulkClient getClient(ClassLoader classLoader) {
        return new HttpBulkClient(classLoader);
    }
}
