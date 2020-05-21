package org.xbib.elx.api;

@FunctionalInterface
public interface BulkClientProvider<C extends BulkClient> {

    C getClient();
}
