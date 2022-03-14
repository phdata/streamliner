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
package io.phdata.streamliner.schemadefiner;

import io.phdata.streamliner.schemadefiner.model.Configuration;
import io.phdata.streamliner.schemadefiner.util.StreamlinerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import schemacrawler.crawl.SchemaDefinerHelper;
import schemacrawler.crawl.StreamlinerCatalog;

/* @deprecated use of StreamlinerConfigreader should be avoided. It was implemented to read config file generated after schema command.
 * But now since we are generating config file per table it does not have Source detail which is needed for  StreamlinerConfigReader */
@Deprecated
public class StreamlinerConfigReader implements SchemaDefiner {
  private static final Logger log = LoggerFactory.getLogger(StreamlinerConfigReader.class);
  private String configFilePath;

  public StreamlinerConfigReader(String configPath) {
    this.configFilePath = configPath;
  }

  @Override
  public StreamlinerCatalog retrieveSchema() {
    log.info("Retrieving Schema from path: {}", configFilePath);
    if (!StreamlinerUtil.fileExists(configFilePath)) {
      throw new RuntimeException(String.format("Configuration File not found: %s", configFilePath));
    }
    Configuration config = StreamlinerUtil.readYamlFile(configFilePath);
    return SchemaDefinerHelper.mapTableDefToStreamlinerCatalog(config);
  }
}
