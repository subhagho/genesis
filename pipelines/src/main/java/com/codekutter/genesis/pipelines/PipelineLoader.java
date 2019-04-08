package com.codekutter.genesis.pipelines;

import com.codekutter.zconfig.common.ConfigProviderFactory;
import com.codekutter.zconfig.common.ConfigurationAnnotationProcessor;
import com.codekutter.zconfig.common.ConfigurationException;
import com.codekutter.zconfig.common.LogUtils;
import com.codekutter.zconfig.common.model.Configuration;
import com.codekutter.zconfig.common.model.ConfigurationSettings;
import com.codekutter.zconfig.common.model.Version;
import com.codekutter.zconfig.common.model.annotations.ConfigAttribute;
import com.codekutter.zconfig.common.model.annotations.ConfigPath;
import com.codekutter.zconfig.common.model.annotations.ConfigValue;
import com.codekutter.zconfig.common.model.nodes.*;
import com.codekutter.zconfig.common.parsers.AbstractConfigParser;
import com.codekutter.zconfig.common.readers.AbstractConfigReader;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Data;
import lombok.ToString;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Loader class to read and parse pipeline definitions from a
 * configuration.
 * <p>
 * Note: Only XML based configurations are supported
 * for pipeline definitions.
 */
public class PipelineLoader {
    /**
     * Struct to read pipeline definition from the configuration.
     */
    @ConfigPath(path = "pipeline")
    @Data
    @ToString
    private static class PipelineDef {
        /**
         * Pipeline name. (Must be unique within a loader context)
         */
        @ConfigAttribute(name = "name", required = true)
        private String name;
        /**
         * Pipeline Entity Type (Canonical class name).
         */
        @ConfigAttribute(name = "entityType", required = true)
        private String entityType;
        /**
         * Pipeline class (Canonical class name).
         */
        @ConfigAttribute(name = "type", required = true)
        private String type;
    }

    /**
     * Struct to read processor definition from the configuration.
     */
    @ConfigPath(path = "processor")
    @Data
    @ToString
    private static class ProcessorDef {
        /**
         * Processor name. (Must be unique within a pipeline)
         */
        @ConfigAttribute(name = "name", required = true)
        private String name;
        /**
         * Processor class (Canonical class name).
         */
        @ConfigAttribute(name = "type", required = true)
        private String type;
        /**
         * Pipeline Entity Type (Canonical class name).
         */
        @ConfigAttribute(name = "entityType", required = true)
        private String entityType;
        /**
         * Condition Query String (where clause expressed in
         * SQL Syntax on the Entity)
         */
        @ConfigValue(name = "condition", required = false)
        private String condition;
        /**
         * Reference Pipeline name - If pipeline is being
         * embedded into another pipeline.
         */
        @ConfigAttribute(name = "reference", required = false)
        private String reference;
    }

    private static final String CONFIG_NODE_PIPELINES = "pipelines";
    private static final String CONFIG_NODE_PIPELINE = "pipeline";
    private static final String CONFIG_NODE_PROCESSORS = "processors";
    private static final String CONFIG_NODE_PROCESSOR = "processor";
    private static final String CONFIG_NODE_ERROR_PS = "errorHandlers";
    private static final String CONFIG_NODE_ERROR_P = "errorHandler";
    private static final String CONFIG_ATTR_ERROR_H_TYPE = "type";

    private Map<String, Pipeline<?>> pipelines = new HashMap<>();

    /**
     * Load the defined pipelines from the passed configuration.
     *
     * @param configName - Configuration Name.
     * @param configUri  - Configuration URI (local file or remote URL)
     * @param version    - Configuration Version (expected)
     * @param settings   - Configuration Settings.
     * @throws ConfigurationException
     */
    public void load(@Nonnull String configName,
                     @Nonnull String configUri, @Nonnull Version version,
                     ConfigurationSettings settings) throws ConfigurationException {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(configName));
        Preconditions.checkArgument(!Strings.isNullOrEmpty(configUri));
        Preconditions.checkArgument(version != null);

        Configuration configuration =
                readConfig(configName, configUri, version, settings);
        Preconditions.checkNotNull(configuration);

        readPipelines(configuration);
    }

    /**
     * Read and parse the pipeline definitions.
     *
     * @param configuration - Configuration handle.
     * @throws ConfigurationException
     */
    private void readPipelines(Configuration configuration)
    throws ConfigurationException {
        AbstractConfigNode node =
                configuration.find(String.format("*.%s", CONFIG_NODE_PIPELINES));
        if (node == null) {
            LogUtils.warn(getClass(),
                          String.format("No pipelines found. [config=%s]",
                                        configuration.getName()));
            return;
        }
        if (!(node instanceof ConfigPathNode) &&
                !(node instanceof ConfigListElementNode)) {
            throw new ConfigurationException(
                    String.format("Invalid Configuration Node: [path=%s][type=%s]",
                                  node.getSearchPath(),
                                  node.getClass().getCanonicalName()));
        }
        if (node instanceof ConfigPathNode) {
            AbstractConfigNode pnode =
                    ((ConfigPathNode) node).getChildNode(CONFIG_NODE_PIPELINE);
            if (pnode instanceof ConfigPathNode) {
                PipelineDef def = ConfigurationAnnotationProcessor
                        .readConfigAnnotations(PipelineDef.class,
                                               (ConfigPathNode) pnode);
                if (def == null) {
                    throw new ConfigurationException(String.format(
                            "Error reading pipeline definition: [path=%s][type=%s]",
                            pnode.getSearchPath(),
                            pnode.getClass().getCanonicalName()));
                }
                readPipeline(def, (ConfigPathNode) pnode);
            } else {
                throw new ConfigurationException(
                        String.format(
                                "Invalid Configuration Node: [path=%s][type=%s]",
                                pnode.getSearchPath(),
                                pnode.getClass().getCanonicalName()));
            }
        } else {
            ConfigListElementNode nodeList = (ConfigListElementNode) node;
            List<ConfigElementNode> values = nodeList.getValues();
            if (values != null && !values.isEmpty()) {
                for (ConfigElementNode elem : values) {
                    if (elem.getName().compareTo(CONFIG_NODE_PIPELINE) == 0) {
                        PipelineDef def = ConfigurationAnnotationProcessor
                                .readConfigAnnotations(PipelineDef.class,
                                                       (ConfigPathNode) elem);
                        if (def == null) {
                            throw new ConfigurationException(String.format(
                                    "Error reading pipeline definition: [path=%s][type=%s]",
                                    node.getSearchPath(),
                                    node.getClass().getCanonicalName()));
                        }
                        readPipeline(def, (ConfigPathNode) elem);
                    }
                }
            }
        }
    }

    /**
     * Parse a pipeline definition from the node.
     *
     * @param def  - Pipeline Definition
     * @param node - Configuration Node.
     * @throws ConfigurationException
     */
    @SuppressWarnings("unchecked")
    private void readPipeline(PipelineDef def, ConfigPathNode node)
    throws ConfigurationException {
        try {
            Class<?> cls = Class.forName(def.type);
            Object obj = ConfigurationAnnotationProcessor
                    .readConfigAnnotations(cls, node);
            if (obj == null) {
                throw new ConfigurationException(
                        "Annotation processor returned a NULL object");
            }
            if (!(obj instanceof Processor<?>)) {
                throw new ConfigurationException(
                        String.format("Invalid Pipeline Type: [type=%s]",
                                      obj.getClass().getCanonicalName()));
            }
            Processor<?> pipeline = (Processor<?>) obj;
            pipeline.setName(def.name);
            Class<?> eType = Class.forName(def.entityType);
            if (pipeline instanceof BasicPipeline<?>) {
                ((BasicProcessor) pipeline).setType(eType);
            } else if (pipeline instanceof CollectionPipeline<?>) {
                ((CollectionPipeline) pipeline).setType(eType);
            }
            readProcessors((Pipeline<?>) pipeline, node);

            AbstractConfigNode enode = node.getChildNode(CONFIG_NODE_ERROR_PS);
            if (enode != null) {
                if (enode instanceof ConfigPathNode) {
                    readErrorHandlers((Pipeline<?>) pipeline,
                                      enode);
                }
            }
            pipelines.put(pipeline.name, (Pipeline<?>) pipeline);
            LogUtils.info(getClass(),
                          String.format("Added pipeline : [name=%s][type=%s]",
                                        pipeline.name,
                                        pipeline.getClass().getCanonicalName()));
        } catch (ClassNotFoundException e) {
            throw new ConfigurationException(e);
        }
    }

    /**
     * Read all the exception handlers specified in the configuration for
     * a pipeline.
     *
     * @param pipeline - Parent Pipeline
     * @param node - Configuration Node.
     * @throws ConfigurationException
     */
    private void readErrorHandlers(Pipeline<?> pipeline,
                                   AbstractConfigNode node)
    throws ConfigurationException {

        if (node instanceof ConfigPathNode) {
            AbstractConfigNode cnode =
                    ((ConfigPathNode) node).getChildNode(CONFIG_NODE_ERROR_P);
            readErrorHandler(pipeline, node);
        } else if (node instanceof ConfigListElementNode) {
            ConfigListElementNode nodeList = (ConfigListElementNode) node;
            List<ConfigElementNode> nodes = nodeList.getValues();
            if (nodes != null && !((ConfigListElementNode) node).isEmpty()) {
                for (ConfigElementNode elem : nodes) {
                    if (elem instanceof ConfigPathNode &&
                            elem.getName().compareTo(CONFIG_NODE_ERROR_P) == 0) {
                        readErrorHandler(pipeline, elem);
                    }
                }
            }
        }

    }

    /**
     * Read an exception handler from the configuration.
     *
     * @param pipeline - Parent Pipeline.
     * @param node     - Configuration node.
     * @throws ConfigurationException
     */
    private void readErrorHandler(Pipeline<?> pipeline,
                                  AbstractConfigNode node)
    throws ConfigurationException {
        try {
            if (node instanceof ConfigPathNode) {
                ConfigAttributesNode attrs =
                        ((ConfigPathNode) node).attributes();
                if (attrs == null) {
                    throw new ConfigurationException(
                            String.format(
                                    "Required Attribute not found. [path=%s]",
                                    node.getSearchPath()));
                }
                ConfigValueNode vn = attrs.getValue(CONFIG_ATTR_ERROR_H_TYPE);
                if (vn == null) {
                    throw new ConfigurationException(
                            String.format(
                                    "Required Attribute not found. [path=%s][attribute=%s]",
                                    node.getSearchPath(),
                                    CONFIG_ATTR_ERROR_H_TYPE));
                }
                String type = vn.getValue();
                Class<?> cls = Class.forName(type);
                Object ep = ConfigurationAnnotationProcessor
                        .readConfigAnnotations(cls, (ConfigPathNode) node);
                if (pipeline instanceof BasicPipeline<?>) {
                    if (ep instanceof ExceptionProcessor<?>) {
                        ExceptionProcessor<?> processor =
                                (ExceptionProcessor<?>) ep;
                        processor.setType(pipeline.getType());
                        processor = ConfigurationAnnotationProcessor
                                .readConfigAnnotations(processor.getClass(),
                                                       (ConfigPathNode) node,
                                                       processor);
                        ((BasicPipeline<?>) pipeline).addErrorHandler(processor);
                        LogUtils.debug(getClass(), String.format(
                                "Added exception processor. [type=%s]",
                                ep.getClass().getCanonicalName()));
                    } else {
                        throw new ConfigurationException(String.format(
                                "Invalid Exception Processor: [type=%s]",
                                ep.getClass().getCanonicalName()));
                    }
                }
            } else {
                throw new ConfigurationException(
                        String.format("Invalid Node type: [type=%s][path=%s]",
                                      node.getClass().getCanonicalName(),
                                      node.getSearchPath()));
            }
        } catch (ClassNotFoundException e) {
            throw new ConfigurationException(e);
        }
    }

    /**
     * Read and parse the processor definitions for the pipeline.
     *
     * @param pipeline - Parent Pipeline.
     * @param node     - Configuration node.
     * @throws ConfigurationException
     */
    private void readProcessors(Pipeline<?> pipeline, ConfigPathNode node)
    throws ConfigurationException {
        AbstractConfigNode pnode =
                node.find(String.format("*.%s", CONFIG_NODE_PROCESSORS));
        if (pnode == null) {
            LogUtils.warn(getClass(),
                          String.format("No pipelines found. [path=%s]",
                                        node.getSearchPath()));
            return;
        }
        if (!(pnode instanceof ConfigPathNode) &&
                !(pnode instanceof ConfigListElementNode)) {
            throw new ConfigurationException(
                    String.format("Invalid Configuration Node: [path=%s][type=%s]",
                                  pnode.getSearchPath(),
                                  pnode.getClass().getCanonicalName()));
        }
        if (pnode instanceof ConfigPathNode) {
            AbstractConfigNode cnode =
                    ((ConfigPathNode) pnode).getChildNode(CONFIG_NODE_PIPELINE);
            if (cnode instanceof ConfigPathNode) {
                ProcessorDef def = ConfigurationAnnotationProcessor
                        .readConfigAnnotations(ProcessorDef.class,
                                               (ConfigPathNode) cnode);
                if (def == null) {
                    throw new ConfigurationException(String.format(
                            "Error reading processor definition: [path=%s][type=%s]",
                            node.getSearchPath(),
                            node.getClass().getCanonicalName()));
                }
                readProcessor(pipeline, def, (ConfigPathNode) cnode);
            } else {
                throw new ConfigurationException(
                        String.format(
                                "Invalid Configuration Node: [path=%s][type=%s]",
                                pnode.getSearchPath(),
                                pnode.getClass().getCanonicalName()));
            }
        } else {
            ConfigListElementNode nodeList = (ConfigListElementNode) pnode;
            List<ConfigElementNode> values = nodeList.getValues();
            if (values != null && !values.isEmpty()) {
                for (ConfigElementNode elem : values) {
                    if (elem.getName().compareTo(CONFIG_NODE_PROCESSOR) == 0) {
                        ProcessorDef def = ConfigurationAnnotationProcessor
                                .readConfigAnnotations(ProcessorDef.class,
                                                       (ConfigPathNode) elem);
                        if (def == null) {
                            throw new ConfigurationException(String.format(
                                    "Error reading processor definition: [path=%s][type=%s]",
                                    node.getSearchPath(),
                                    node.getClass().getCanonicalName()));
                        }
                        readProcessor(pipeline, def, (ConfigPathNode) elem);
                    }
                }
            }
        }
    }

    /**
     * Parse the processor definition and add it to the parent pipeline.
     *
     * @param pipeline - Parent Pipeline.
     * @param def      - Processor Definition.
     * @param node     - Configuration node.
     * @throws ConfigurationException
     */
    @SuppressWarnings("unchecked")
    private void readProcessor(Pipeline<?> pipeline, ProcessorDef def,
                               ConfigPathNode node) throws ConfigurationException {
        Processor<?> processor = null;
        try {
            if (!Strings.isNullOrEmpty(def.reference)) {
                Pipeline<?> ref = getPipeline(def.reference);
                if (ref == null) {
                    throw new ConfigurationException(
                            String.format("No pipeline reference found. [name=%s]",
                                          def.reference));
                }
                processor = (Processor<?>) ref;
            } else {
                Class<?> cls = Class.forName(def.type);
                Object obj = ConfigurationAnnotationProcessor
                        .readConfigAnnotations(cls, node);
                if (obj == null) {
                    throw new ConfigurationException(
                            "Annotation processor returned a NULL object");
                }
                if (!(obj instanceof Processor<?>)) {
                    throw new ConfigurationException(
                            String.format("Invalid Pipeline Type: [type=%s]",
                                          obj.getClass().getCanonicalName()));
                }
                processor = (Processor<?>) obj;
                processor.setName(def.name);
                Class<?> eType = Class.forName(def.entityType);
                if (processor instanceof BasicProcessor<?>) {
                    ((BasicProcessor) processor).setType(eType);
                } else if (processor instanceof CollectionProcessor<?>) {
                    ((CollectionPipeline) processor).setType(eType);
                }
            }

            if (pipeline instanceof BasicPipeline<?>) {
                ((BasicPipeline<?>) pipeline)
                        .addProcessor((BasicProcessor<?>) processor,
                                      def.condition);
            } else if (pipeline instanceof CollectionPipeline<?>) {
                ((CollectionPipeline) pipeline)
                        .addProcessor((CollectionProcessor<?>) processor,
                                      def.condition);
            }
        } catch (ClassNotFoundException e) {
            throw new ConfigurationException(e);
        }
    }

    /**
     * Get an instance of a pipeline.
     *
     * @param name - Pipeline name.
     * @param <T>  - Entity Type.
     * @return - Pipeline instance.
     */
    @SuppressWarnings("unchecked")
    public <T> Pipeline<T> getPipeline(String name) {
        return (Pipeline<T>) pipelines.get(name);
    }

    /**
     * Read and load the configuration from the specified URI.
     *
     * @param configName - Configuration name.
     * @param configUri  - Configuration URI.
     * @param version    - Configuration Version.
     * @param settings   - Configuration settings.
     * @return - Configuration instance.
     * @throws ConfigurationException
     */
    private Configuration readConfig(String configName,
                                     String configUri,
                                     Version version,
                                     ConfigurationSettings settings) throws
                                                                     ConfigurationException {
        try {
            URI uri = new URI(configUri);
            ConfigProviderFactory.EConfigType configType =
                    ConfigProviderFactory.EConfigType.XML;
            LogUtils.debug(getClass(),
                           String.format(
                                   "Reading configuration: [type=%s][uri=%s][version=%s]",
                                   configType.name(), uri.toString(),
                                   version.toString()));
            try (AbstractConfigReader reader = ConfigProviderFactory.reader(uri)) {
                if (reader == null) {
                    throw new ConfigurationException(
                            String.format("Error getting reader for URI: [uri=%s]",
                                          configUri));
                }
                try (
                        AbstractConfigParser parser =
                                ConfigProviderFactory.parser(configType)) {
                    if (parser == null) {
                        throw new ConfigurationException(
                                String.format(
                                        "Error getting parser for type: [type=%s]",
                                        configType.name()));
                    }
                    parser.parse(configName, reader, settings, version);
                    return parser.getConfiguration();
                }
            }
        } catch (URISyntaxException e) {
            throw new ConfigurationException(e);
        } catch (IOException e) {
            throw new ConfigurationException(e);
        }
    }
}
