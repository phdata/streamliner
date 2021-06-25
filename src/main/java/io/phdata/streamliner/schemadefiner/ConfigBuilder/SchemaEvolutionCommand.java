package io.phdata.streamliner.schemadefiner.ConfigBuilder;

import io.phdata.streamliner.schemadefiner.model.Configuration;
import io.phdata.streamliner.schemadefiner.model.ConfigurationDiff;
import io.phdata.streamliner.schemadefiner.util.StreamlinerUtil;

import java.io.IOException;

public class SchemaEvolutionCommand {

  private static final String PREVIOUS_STREAMLINER_CONFIGURATION_NAME =
      "previous-streamliner-configuration.yml";
  private static final String CURRENT_STREAMLINER_CONFIGURATION_NAME =
      "current-streamliner-configuration.yml";

  public static void build(
      String previousConfigPath, String currentConfigPath, String outputDirectory)
      throws IOException {
    if (currentConfigPath == null) {
      throw new RuntimeException("Current configuration is mandatory.");
    }
    Configuration prevConfig = StreamlinerUtil.readYamlFile(previousConfigPath);
    Configuration currConfig = StreamlinerUtil.readYamlFile(currentConfigPath);
    ConfigurationDiff configDiff = DiffGenerator.createConfigDiff(prevConfig, currConfig);
    // writing configuration difference file
    StreamlinerUtil.writeConfigToYaml(configDiff, outputDirectory);
    // writing previous configuration file
    StreamlinerUtil.writeConfigToYaml(
        prevConfig, outputDirectory, PREVIOUS_STREAMLINER_CONFIGURATION_NAME);
    // writing current configuration file
    StreamlinerUtil.writeConfigToYaml(
        currConfig, outputDirectory, CURRENT_STREAMLINER_CONFIGURATION_NAME);
  }
}
