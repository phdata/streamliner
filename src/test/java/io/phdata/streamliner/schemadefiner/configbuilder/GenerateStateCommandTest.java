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

import static org.junit.Assert.assertEquals;

import io.phdata.streamliner.App;
import java.io.File;
import org.junit.Test;

public class GenerateStateCommandTest {

  private final String tableCsvFile = "src/test/resources/csv/Snowflake-Tables.csv";
  private final String columnCsvFile = "src/test/resources/csv/Snowflake-Columns.csv";
  private String outputPath = "src/test/resources/results/generateStateCommand";
  private final String sourceStateDirectory = "src/test/resources/csv/source-state-directory";

  @Test
  public void testGenerateStateFile_no_regex() {
    String outputDir = String.format("%s/%s", outputPath, "testGenerateStateFile_no_regex");
    String generateStateCommand[] = {
      "generate-state",
      "--table-csv-file",
      tableCsvFile,
      "--column-csv-file",
      columnCsvFile,
      "--output-path",
      outputDir,
      "--source-state-directory",
      sourceStateDirectory
    };
    App.main(generateStateCommand);

    File f = new File(outputDir);
    assertEquals(5, f.list().length);
  }

  @Test
  public void testGenerateStateFile_table_exclude() {
    String outputDir = String.format("%s/%s", outputPath, "testGenerateStateFile_table_exclude");
    String generateStateCommand[] = {
      "generate-state",
      "--table-csv-file",
      tableCsvFile,
      "--column-csv-file",
      columnCsvFile,
      "--output-path",
      outputDir,
      "--source-state-directory",
      sourceStateDirectory,
      "--table-exclude",
      "TABLE_NAME"
    };
    App.main(generateStateCommand);

    File f = new File(outputDir);
    assertEquals(4, f.list().length);
  }

  @Test
  public void testGenerateStateFile_table_include() {
    String outputDir = String.format("%s/%s", outputPath, "testGenerateStateFile_table_include");
    String generateStateCommand[] = {
      "generate-state",
      "--table-csv-file",
      tableCsvFile,
      "--column-csv-file",
      columnCsvFile,
      "--output-path",
      outputDir,
      "--source-state-directory",
      sourceStateDirectory,
      "--table-include",
      "STREAMLINER_TEST2"
    };
    App.main(generateStateCommand);

    File f = new File(outputDir);
    assertEquals(1, f.list().length);
  }
}
