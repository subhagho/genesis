package com.codekutter.genesis.pipelines;

/**
 * Interface to be implemented by Data Pipelines.
 *
 * @param <T> - Entity Type
 */
public interface Pipeline<T> {
    /**
     * Get the state of this pipeline.
     *
     * @return - Pipeline State
     */
    EProcessState getState();

    /**
     * Dispose this instance of the pipeline.
     */
    void dispose();
}
