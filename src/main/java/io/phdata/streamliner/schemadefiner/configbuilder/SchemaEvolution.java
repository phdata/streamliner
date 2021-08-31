package io.phdata.streamliner.schemadefiner.configbuilder;

import io.phdata.streamliner.schemadefiner.model.Configuration;
import io.phdata.streamliner.schemadefiner.model.ConfigurationDiff;
import io.phdata.streamliner.schemadefiner.util.StreamlinerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SchemaEvolution {

  private static final Logger log = LoggerFactory.getLogger(SchemaEvolution.class);

  public static void build(
          Configuration prevConfig, Configuration currConfig, String diffOutputFile) {
    if (currConfig == null) {
      throw new RuntimeException("Current configuration is mandatory.");
    }
    ConfigurationDiff configDiff = DiffGenerator.createConfigDiff(prevConfig, currConfig);
    log.info("Successfully calculated the configuration differences.");
    // writing configuration difference file
    StreamlinerUtil.writeConfigToYaml(configDiff, diffOutputFile);
    log.debug(
            "Configuration difference file, previous configuration file and current configuration file is written to: {}",
            StreamlinerUtil.getOutputDirectory(diffOutputFile));
  }
}
