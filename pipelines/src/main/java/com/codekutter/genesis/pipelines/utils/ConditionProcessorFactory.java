package com.codekutter.genesis.pipelines.utils;

import java.util.HashMap;
import java.util.Map;

/**
 * Factory class to create Condition Processors.
 * Query parsers are expected to be singleton instances per type.
 * Initializing multiple instances for the same type will throw exception.
 */
public class ConditionProcessorFactory {
    private static final Map<Class<?>, ConditionProcessor<?>> processors =
            new HashMap<>();

    /**
     * Get/Create an instance of the condition processor for the passed type.
     *
     * @param type - Entity Class.
     * @param <T>  - Entity Type.
     * @return - Condition Processor instance.
     */
    @SuppressWarnings("unchecked")
    public static <T> ConditionProcessor<T> getProcessor(Class<T> type) {
        synchronized (processors) {
            ConditionProcessor<T> processor = null;
            if (!processors.containsKey(type)) {
                processor = new ConditionProcessor<T>(type);
                processors.put(type, processor);
            }

            return (ConditionProcessor<T>) processors.get(type);
        }
    }
}
