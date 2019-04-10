package com.codekutter.genesis.pipelines.processors;

import com.codekutter.genesis.pipelines.BasicProcessor;
import com.codekutter.genesis.pipelines.Context;
import com.codekutter.genesis.pipelines.EProcessorResponse;
import com.codekutter.genesis.pipelines.ProcessorResponse;
import com.codekutter.zconfig.common.ConfigurationException;
import com.codekutter.zconfig.common.model.nodes.AbstractConfigNode;
import com.google.common.base.Preconditions;

import javax.annotation.Nonnull;

public class EntityStateFilter extends BasicProcessor<DemoEntity> {
    /**
     * Execute method to be implemented for processing the data passed.
     *
     * @param data     - Entity Object.
     * @param context  - Context Handle
     * @param response - Processor Response.
     * @return - Processor Response.
     */
    @Override
    protected ProcessorResponse<DemoEntity> execute(@Nonnull DemoEntity data,
                                                    Context context, @Nonnull
                                                            ProcessorResponse<DemoEntity> response) {
        Preconditions.checkArgument(data != null);
        if (data.getActive() == EDemo.Deleted ||
                data.getActive() == EDemo.Unknown) {
            response.setData(null);
            response.setState(EProcessorResponse.NullData);
        }
        return response;
    }

    /**
     * Method to initialize the processor from the configuration.
     * <p>
     * Note: Use the MethodInvoke annotation with the required path
     * to auto-wire the initialisation.
     *
     * @param node - Configuration Node.
     * @throws ConfigurationException
     */
    @Override
    public void init(AbstractConfigNode node) throws ConfigurationException {
        // Do nothing.
    }
}
