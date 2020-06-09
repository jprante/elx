module org.xbib.elx.node {
    exports org.xbib.elx.node;
    requires org.xbib.elx.api;
    requires org.xbib.elx.common;
    requires org.xbib.elasticsearch.log4j;
    requires org.xbib.elasticsearch.server;
    requires org.xbib.elasticsearch.transport.nettyfour;
    provides org.xbib.elx.api.AdminClientProvider with
            org.xbib.elx.node.NodeAdminClientProvider;
    provides org.xbib.elx.api.BulkClientProvider with
            org.xbib.elx.node.NodeBulkClientProvider;
    provides org.xbib.elx.api.SearchClientProvider with
            org.xbib.elx.node.NodeSearchClientProvider;
}
