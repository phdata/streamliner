/*
 * Copyright 2018 phData Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.phdata.pipewrench.pipeline

import io.phdata.pipewrench.configuration.YamlSupport
import org.scalatest.FunSuite

class PipelineBuilderTest extends FunSuite with YamlSupport {
  test("Render truncate-reload templates") {
    val configuration = readConfigurationFile("src/test/resources/truncate-reload.yml")
    val typeMapping = readTypeMappingFile("src/main/resources/type-mapping.yml")
    PipelineBuilder
      .build(configuration, typeMapping, "src/main/resources/templates/", "target/truncate-reload")
  }

  test("Render kudu-table-ddl templates") {
    val configuration = readConfigurationFile("src/test/resources/kudu-table-ddl.yml")
    val typeMapping = readTypeMappingFile("src/main/resources/type-mapping.yml")
    PipelineBuilder
      .build(configuration, typeMapping, "src/main/resources/templates/", "target/kudu-table-ddl")
  }

  test("Render incremental-with-kudu templates") {
    val configuration = readConfigurationFile("src/test/resources/incremental-with-kudu.yml")
    val typeMapping = readTypeMappingFile("src/main/resources/type-mapping.yml")
    PipelineBuilder.build(
      configuration,
      typeMapping,
      "src/main/resources/templates/",
      "target/incremental-with-kudu")
  }

}