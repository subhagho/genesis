package com.codekutter.genesis.pipelines.types;

/**
 * Interface definition for implementing entity transformers.
 *
 * @param <S> - Source Entity Type.
 * @param <T> - Target Entity Type.
 */
public interface IEntityTransformer<S, T> {
    /**
     * Transformation handler to transform entity from source type to target type.
     *
     * @param source - Source entity to transform.
     * @return - Transformed target entity.
     * @throws TransformtationException
     */
    T transform(S source) throws TransformtationException;
}
