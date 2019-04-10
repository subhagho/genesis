package com.codekutter.genesis.pipelines.extensions;

import com.codekutter.genesis.pipelines.types.Entity;

import javax.annotation.Nonnull;

/**
 * Entity Data Producer - Used to define data producers for keyed entities.
 *
 * @param <T> - Entity Type.
 * @param <K> - Entity Key Type.
 */
public interface IEntityDataProducer<T extends Entity<K>, K>
        extends IDataProducer<T> {
    /**
     * Find an entity for the specified key.
     *
     * @param key - Entity key.
     * @return - Entity instance.
     * @throws DataServiceException
     */
    T find(@Nonnull K key) throws DataServiceException;
}
