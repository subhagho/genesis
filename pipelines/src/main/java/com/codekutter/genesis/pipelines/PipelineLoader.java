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

public class PipelineLoader {
    @ConfigPath(path = "pipeline")
    @Data
    @ToString
    private static class PipelineDef {
        @ConfigAttribute(name = "name", required = true)
        private String name;
        @ConfigAttribute(name = "entityType", required = true)
        private String entityType;
        @ConfigAttribute(name = "type", required = true)
        private String type;
    }

    @ConfigPath(path = "processor")
    @Data
    @ToString
    private static class ProcessorDef {
        @ConfigAttribute(name = "name", required = true)
        private String name;
        @ConfigAttribute(name = "type", required = true)
        private String type;
        @ConfigAttribute(name = "entityType", required = true)
        private String entityType;
        @ConfigValue(name = "condition", required = false)
        private String condition;
        @ConfigAttribute(name = "reference", required = false)
        private String reference;
    }

    private static final String CONFIG_NODE_PIPELINES = "pipelines";
    private static final String CONFIG_NODE_PIPELINE = "pipeline";
    private static final String CONFIG_NODE_PROCESSORS = "processors";
    private static final String CONFIG_NODE_PROCESSOR = "processor";

    private Map<String, Pipeline<?>> pipelines = new HashMap<>();

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
                            node.getSearchPath(),
                            node.getClass().getCanonicalName()));
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
            Processor<?> pipeline = (Processor<?>)obj;
            pipeline.setName(def.name);
            Class<?> eType = Class.forName(def.entityType);
            if (pipeline instanceof BasicPipeline<?>) {
                ((BasicProcessor) pipeline).setType(eType);
            } else if (pipeline instanceof CollectionPipeline<?>) {
                ((CollectionPipeline) pipeline).setType(eType);
            }
            
        } catch (ClassNotFoundException e) {
            throw new ConfigurationException(e);
        }
    }

    @SuppressWarnings("unchecked")
    public <T> Pipeline<T> getPipeline(String name) {
        return (Pipeline<T>) pipelines.get(name);
    }

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
