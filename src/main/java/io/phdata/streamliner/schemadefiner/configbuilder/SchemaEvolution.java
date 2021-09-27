// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
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
