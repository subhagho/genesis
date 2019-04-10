package com.codekutter.genesis.pipelines.processors;

import com.codekutter.genesis.pipelines.ExceptionProcessor;
import com.codekutter.genesis.pipelines.ProcessorResponse;
import com.codekutter.zconfig.common.LogUtils;

import javax.annotation.Nonnull;

public class DemoExceptionProcessor extends ExceptionProcessor<DemoEntity> {
    /**
     * Handler function to be implemented to handle the error condition.
     *
     * @param errorResponse - Response with Error.
     * @return - Handler Response.
     */
    @Override
    protected ProcessorResponse<DemoEntity> handle(
            @Nonnull ProcessorResponse<DemoEntity> errorResponse) {
        LogUtils.error(getType(), String.format("Error Handler: [error=%s]",
                                                errorResponse.getError()
                                                             .getLocalizedMessage()));
        return errorResponse;
    }
}
