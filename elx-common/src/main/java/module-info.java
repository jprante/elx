module org.xbib.elx.common {
    uses org.xbib.elx.api.AdminClientProvider;
    uses org.xbib.elx.api.BulkClientProvider;
    uses org.xbib.elx.api.SearchClientProvider;
    exports org.xbib.elx.common;
    exports org.xbib.elx.common.io;
    exports org.xbib.elx.common.util;
    requires org.xbib.elx.api;
    requires org.xbib.metrics.api;
    requires org.xbib.metrics.common;
    requires org.xbib.elasticsearch.hppc;
    requires org.xbib.elasticsearch.log4j;
    requires org.xbib.elasticsearch.server;
    provides java.net.URLStreamHandlerFactory with
            org.xbib.elx.common.io.ClasspathURLStreamHandlerFactory;
    provides org.xbib.elx.api.AdminClientProvider with
            org.xbib.elx.common.MockAdminClientProvider;
    provides org.xbib.elx.api.BulkClientProvider with
            org.xbib.elx.common.MockBulkClientProvider;
    provides org.xbib.elx.api.SearchClientProvider with
            org.xbib.elx.common.MockSearchClientProvider;
}
