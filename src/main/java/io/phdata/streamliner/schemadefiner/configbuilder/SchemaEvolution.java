package io.phdata.streamliner.schemadefiner.configbuilder;

import io.phdata.streamliner.schemadefiner.model.Configuration;
import io.phdata.streamliner.schemadefiner.model.ConfigurationDiff;
import io.phdata.streamliner.schemadefiner.util.StreamlinerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SchemaEvolution {

  private static final Logger log = LoggerFactory.getLogger(SchemaEvolution.class);

  private static final String PREVIOUS_STREAMLINER_CONFIGURATION_NAME =
      "previous-streamliner-configuration.yml";
  private static final String CURRENT_STREAMLINER_CONFIGURATION_NAME =
      "current-streamliner-configuration.yml";

  public static void build(
      String previousConfigPath, String currentConfigPath, String diffOutputFile) {
    if (currentConfigPath == null) {
      throw new RuntimeException("Current configuration is mandatory.");
    }
    Configuration prevConfig = StreamlinerUtil.readYamlFile(previousConfigPath);
    Configuration currConfig = StreamlinerUtil.readYamlFile(currentConfigPath);
    ConfigurationDiff configDiff = DiffGenerator.createConfigDiff(prevConfig, currConfig);
    log.info("Successfully calculated the configuration differences.");
    // writing configuration difference file
    StreamlinerUtil.writeConfigToYaml(configDiff, diffOutputFile);
    String outputDir = StreamlinerUtil.getOutputDirectory(diffOutputFile);
    // writing previous configuration file
    StreamlinerUtil.writeConfigToYaml(
            prevConfig,
            StreamlinerUtil.getOutputDirectory(diffOutputFile),
            String.format("%s/%s", outputDir, PREVIOUS_STREAMLINER_CONFIGURATION_NAME));
    // writing current configuration file
    StreamlinerUtil.writeConfigToYaml(
            currConfig,
            StreamlinerUtil.getOutputDirectory(diffOutputFile),
            String.format("%s/%s", outputDir, CURRENT_STREAMLINER_CONFIGURATION_NAME));
    log.debug(
            "Configuration difference file, previous configuration file and current configuration file is written to: {}",
            StreamlinerUtil.getOutputDirectory(diffOutputFile));
  }
}
