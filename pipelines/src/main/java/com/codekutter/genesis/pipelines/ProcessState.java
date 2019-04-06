package com.codekutter.genesis.pipelines;

import com.codekutter.zconfig.common.AbstractState;

/**
 * State handle for storing the state/exceptions, for the process instance.
 */
public class ProcessState extends AbstractState<EProcessState> {
    public boolean isAvailable() {
        return (getState() == EProcessState.Available);
    }

    public  boolean isInitialized() {
        return (getState() == EProcessState.Initialized);
    }

    public boolean isDisposed() {
        return (getState() == EProcessState.Disposed);
    }


}
