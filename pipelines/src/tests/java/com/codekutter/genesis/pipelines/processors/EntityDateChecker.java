package com.codekutter.genesis.pipelines.processors;

import com.codekutter.genesis.pipelines.BasicProcessor;
import com.codekutter.genesis.pipelines.Context;
import com.codekutter.genesis.pipelines.EProcessState;
import com.codekutter.genesis.pipelines.ProcessorResponse;
import com.codekutter.zconfig.common.ConfigurationException;
import com.codekutter.zconfig.common.LogUtils;
import com.codekutter.zconfig.common.model.nodes.AbstractConfigNode;
import com.codekutter.zconfig.common.model.nodes.ConfigAttributesNode;
import com.codekutter.zconfig.common.model.nodes.ConfigPathNode;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import javax.annotation.Nonnull;
import java.text.SimpleDateFormat;
import java.util.Date;

public class EntityDateChecker extends BasicProcessor<DemoEntity> {
    private static final String CONFIG_ATTR_CUTOFF_DATE = "cutOffDate";

    private Date cutOffDate = null;

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
        Date dt = data.getDateTime();
        if (dt.before(cutOffDate)) {
            data.setActive(EDemo.InActive);
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
        if (!(node instanceof ConfigPathNode)) {
            throw new ConfigurationException(
                    String.format("Invalid Node : [type=%s][path=%s]",
                                  node.getClass().getCanonicalName(),
                                  node.getSearchPath()));
        }
        ConfigAttributesNode attrs = ((ConfigPathNode) node).attributes();
        Preconditions.checkNotNull(attrs);

        try {
            String dateS = attrs.getValue(CONFIG_ATTR_CUTOFF_DATE).getValue();
            Preconditions.checkState(!Strings.isNullOrEmpty(dateS));
            SimpleDateFormat df = new SimpleDateFormat("MM-dd-yyyy");
            cutOffDate = df.parse(dateS);
            LogUtils.debug(getClass(),
                           String.format("Using Cutoff Date: [%s]", dateS));
            state.setState(EProcessState.Available);
        } catch (Exception e) {
            state.setError(e);
            throw new ConfigurationException(e);
        }
    }
}
