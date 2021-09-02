package io.phdata.streamliner.schemadefiner.configbuilder;

import io.phdata.streamliner.App;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertEquals;

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
