package com.codekutter.genesis.pipelines.utils;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.googlecode.cqengine.ConcurrentIndexedCollection;
import com.googlecode.cqengine.IndexedCollection;
import com.googlecode.cqengine.query.parser.sql.SQLParser;
import com.googlecode.cqengine.resultset.ResultSet;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.googlecode.cqengine.codegen.AttributeBytecodeGenerator.createAttributes;

/**
 * Entity condition processor class.
 * Conditions are represented as SQL Query on the type.
 *
 * @param <T> - Entity Type.
 */
public class ConditionProcessor<T> {
    /**
     * Local Parser instance.
     */
    private SQLParser<T> parser = null;

    /**
     * Constructor with the entity type.
     *
     * @param type - Entity type.
     */
    public ConditionProcessor(Class<T> type) {
        parser = SQLParser.forPojoWithAttributes(type, createAttributes(type));
    }

    /**
     * Check if the entity data passed matches the specified condition.
     *
     * @param data      - Entity Data
     * @param condition - Match condition
     * @return - Matches?
     */
    public boolean matches(@Nonnull T data, @Nonnull String condition) {
        Preconditions.checkNotNull(parser);
        Preconditions.checkArgument(data != null);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(condition));
        String query = getFormattedQuery(data.getClass(), condition);
        IndexedCollection<T> values = new ConcurrentIndexedCollection<>();
        values.add(data);

        ResultSet<T> result = parser.retrieve(values, query);
        if (result != null && result.isNotEmpty()) {
            return true;
        }
        return false;
    }

    /**
     * Filter the entity data list based on the specified query condition.
     *
     * @param data      - Entity Data List
     * @param condition - Filter condition.
     * @return - Filtered List.
     */
    public List<T> filter(Collection<T> data, String condition) {
        Preconditions.checkNotNull(parser);
        Preconditions.checkArgument(data != null && !data.isEmpty());
        Preconditions.checkArgument(!Strings.isNullOrEmpty(condition));
        String query = getFormattedQuery(data.getClass(), condition);
        IndexedCollection<T> values = new ConcurrentIndexedCollection<>();
        values.addAll(data);

        ResultSet<T> result = parser.retrieve(values, query);
        if (result != null && result.isNotEmpty()) {
            List<T> ret = new ArrayList<>();
            for (T tt : result) {
                ret.add(tt);
            }
            return ret;
        }
        return null;
    }

    private String getFormattedQuery(Class<?> type, String condition) {
        String source = type.getSimpleName();
        return String.format("SELECT * FROM %s WHERE (%s)", source, condition);
    }
}
