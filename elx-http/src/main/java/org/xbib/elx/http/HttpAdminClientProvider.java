package org.xbib.elx.http;

import org.xbib.elx.api.AdminClientProvider;

public class HttpAdminClientProvider implements AdminClientProvider<HttpAdminClient> {
    @Override
    public HttpAdminClient getClient() {
        return new HttpAdminClient();
    }
}
