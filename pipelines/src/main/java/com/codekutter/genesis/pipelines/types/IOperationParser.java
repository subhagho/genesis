package com.codekutter.genesis.pipelines.types;

import com.codekutter.genesis.pipelines.ProcessorException;

/**
 * Interface for parsing the operation types.
 *
 * @param <O> - Operation Type.
 */
public interface IOperationParser<O> {
    public static final String CONTEXT_KEY_OPERATION = "data.consumer.operation";

    /**
     * Parse the operation type based on the input context object.
     *
     * @param operation - Operation context object.
     * @return - Operation type.
     * @throws ProcessorException
     */
    O parseOperation(Object operation) throws ProcessorException;
}
