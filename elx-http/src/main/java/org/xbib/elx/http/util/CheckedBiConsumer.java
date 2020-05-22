package org.xbib.elx.http.util;

import java.util.function.BiConsumer;

/**
 * A {@link BiConsumer}-like interface which allows throwing checked exceptions.
 */
@FunctionalInterface
public interface CheckedBiConsumer<T, U, E extends Exception> {
    void accept(T t, U u) throws E;
}
