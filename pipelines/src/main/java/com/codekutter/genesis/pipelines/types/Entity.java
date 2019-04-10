package com.codekutter.genesis.pipelines.types;

/**
 * Interface to implement keyed entities.
 *
 * @param <K> - Key Type
 */
public interface Entity<K> {
    /**
     * Get the Entity Key.
     *
     * @return - Entity Key.
     */
    K getKey();
}
