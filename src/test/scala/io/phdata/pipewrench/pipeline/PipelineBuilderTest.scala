/* Copyright 2018 phData Inc. */

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
