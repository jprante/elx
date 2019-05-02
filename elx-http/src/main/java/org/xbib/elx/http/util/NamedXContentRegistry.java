package org.xbib.elx.http.util;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;

public class NamedXContentRegistry {

    public static class Entry {
        /** The class that this entry can read. */
        public final Class<?> categoryClass;

        /** A name for the entry which is unique within the {@link #categoryClass}. */
        public final ParseField name;

        /** A parser capability of parser the entry's class. */
        private final ContextParser<Object, ?> parser;

        /** Creates a new entry which can be stored by the registry. */
        public <T> Entry(Class<T> categoryClass, ParseField name, CheckedFunction<XContentParser, ? extends T, IOException> parser) {
            this.categoryClass = Objects.requireNonNull(categoryClass);
            this.name = Objects.requireNonNull(name);
            this.parser = Objects.requireNonNull((p, c) -> parser.apply(p));
        }
        /**
         * Creates a new entry which can be stored by the registry.
         * Prefer {@link Entry#Entry(Class, ParseField, CheckedFunction)} unless you need a context to carry around while parsing.
         */
        public <T> Entry(Class<T> categoryClass, ParseField name, ContextParser<Object, ? extends T> parser) {
            this.categoryClass = Objects.requireNonNull(categoryClass);
            this.name = Objects.requireNonNull(name);
            this.parser = Objects.requireNonNull(parser);
        }
    }

    private final Map<Class<?>, Map<String, Entry>> registry;

    public NamedXContentRegistry(List<Entry> entries) {
        if (entries.isEmpty()) {
            registry = emptyMap();
            return;
        }
        entries = new ArrayList<>(entries);
        entries.sort(Comparator.comparing(e -> e.categoryClass.getName()));

        Map<Class<?>, Map<String, Entry>> registry = new HashMap<>();
        Map<String, Entry> parsers = null;
        Class<?> currentCategory = null;
        for (Entry entry : entries) {
            if (currentCategory != entry.categoryClass) {
                if (currentCategory != null) {
                    // we've seen the last of this category, put it into the big map
                    registry.put(currentCategory, unmodifiableMap(parsers));
                }
                parsers = new HashMap<>();
                currentCategory = entry.categoryClass;
            }

            for (String name : entry.name.getAllNamesIncludedDeprecated()) {
                Object old = parsers.put(name, entry);
                if (old != null) {
                    throw new IllegalArgumentException("NamedXContent [" + currentCategory.getName() + "][" + entry.name + "]" +
                        " is already registered for [" + old.getClass().getName() + "]," +
                        " cannot register [" + entry.parser.getClass().getName() + "]");
                }
            }
        }
        // handle the last category
        registry.put(currentCategory, unmodifiableMap(parsers));

        this.registry = unmodifiableMap(registry);
    }

    public <T, C> T parseNamedObject(Class<T> categoryClass, String name, XContentParser parser, C context) throws IOException {
        Map<String, Entry> parsers = registry.get(categoryClass);
        if (parsers == null) {
            if (registry.isEmpty()) {
                // The "empty" registry will never work so we throw a better exception as a hint.
                throw new NamedObjectNotFoundException("named objects are not supported for this parser");
            }
            throw new NamedObjectNotFoundException("unknown named object category [" + categoryClass.getName() + "]");
        }
        Entry entry = parsers.get(name);
        if (entry == null) {
            throw new NamedObjectNotFoundException(parser.getTokenLocation(), "unable to parse " + categoryClass.getSimpleName() +
                " with name [" + name + "]: parser not found");
        }
        return categoryClass.cast(entry.parser.parse(parser, context));
    }

}
