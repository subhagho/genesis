package com.codekutter.genesis.pipelines.processors;

import com.codekutter.genesis.pipelines.*;
import com.codekutter.zconfig.common.ConfigurationException;
import com.codekutter.zconfig.common.LogUtils;
import com.codekutter.zconfig.common.model.nodes.AbstractConfigNode;
import com.codekutter.zconfig.common.model.nodes.ConfigAttributesNode;
import com.codekutter.zconfig.common.model.nodes.ConfigPathNode;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.commons.lang3.RandomStringUtils;

import javax.annotation.Nonnull;

public class EntityNameProcessor extends BasicProcessor<DemoEntity> {
    private static final String CONFIG_ATTR_NAME_PREFIX = "namePrefix";

    private String namePrefix;

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
        if (!(node instanceof ConfigPathNode)) {
            throw new ConfigurationException(
                    String.format("Invalid Node : [type=%s][path=%s]",
                                  node.getClass().getCanonicalName(),
                                  node.getSearchPath()));
        }
        ConfigAttributesNode attrs = ((ConfigPathNode) node).attributes();
        Preconditions.checkNotNull(attrs);

        namePrefix = attrs.getValue(CONFIG_ATTR_NAME_PREFIX).getValue();
        Preconditions.checkState(!Strings.isNullOrEmpty(namePrefix));
        LogUtils.debug(getClass(),
                       String.format("Using Name Prefix: [%s]", namePrefix));
        state.setState(EProcessState.Available);
    }

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
        if (Strings.isNullOrEmpty(data.getName())) {
            String name = String.format("%s %s", namePrefix,
                                        RandomStringUtils.random(20, true, true));
            LogUtils.debug(getClass(),
                           String.format("Setting Entity Name: [%s]", name));
            data.setName(name);
            response.setData(data);
        }
        return response;
    }
}
