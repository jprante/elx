module org.xbib.elx.transport {
    exports org.xbib.elx.transport;
    requires org.xbib.elx.api;
    requires org.xbib.elx.common;
    requires org.xbib.elasticsearch.log4j;
    requires org.xbib.elasticsearch.server;
    requires org.xbib.elasticsearch.transport.nettyfour;
    provides org.xbib.elx.api.AdminClientProvider with
            org.xbib.elx.transport.TransportAdminClientProvider;
    provides org.xbib.elx.api.BulkClientProvider with
            org.xbib.elx.transport.TransportBulkClientProvider;
    provides org.xbib.elx.api.SearchClientProvider with
            org.xbib.elx.transport.TransportSearchClientProvider;
}
