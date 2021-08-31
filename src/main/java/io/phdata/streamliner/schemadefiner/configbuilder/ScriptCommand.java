package io.phdata.streamliner.schemadefiner.configbuilder;

import io.phdata.streamliner.schemadefiner.model.*;
import io.phdata.streamliner.schemadefiner.util.StreamlinerUtil;
import io.phdata.streamliner.util.JavaHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ScriptCommand {
  private static final Logger log = LoggerFactory.getLogger(ScriptCommand.class);

    // config is ingest-configuration.yml
  public static void build(
      String config,
      String stateDirectory,
      String previousStateDirectory,
      String typeMappingFile,
      String templateDirectory,
      String outputDirectory) {

    Configuration ingestConfig = StreamlinerUtil.readConfigFromPath(config);
    if(ingestConfig == null){
        throw new RuntimeException("--config file(example: private-ingest-configuration.yml ) can not be empty or config file path is null.");
    }
    Configuration configuration = StreamlinerUtil.createConfig(stateDirectory, ingestConfig, "--state-directory");
    ConfigurationDiff configDiff = StreamlinerUtil.readConfigDiffFromPath(String.format("%s/%s", stateDirectory, Constants.STREAMLINER_DIFF_FILE.value()));
    if (configuration == null && configDiff == null) {
      throw new RuntimeException("--state-directory has no table config and streamliner-diff.yml not found.");
    }
    if (configuration == null) {
      throw new RuntimeException("--state-directory must have atleast one table config.");
    }
    // --previous-state-directory is mandatory for every run. After every successful run, table config is moved from state-directory to previous-state-directory.
    validatePreviousStateDirectory(previousStateDirectory);

    Map<String, Map<String, String>> typeMapping =
        StreamlinerUtil.readTypeMappingFile(typeMappingFile);
    if (outputDirectory == null || outputDirectory.equals("")) {
      outputDirectory =
          String.format(
              "output/%s/%s/scripts", configuration.getName(), configuration.getEnvironment());
      log.info("Invalid --output-directory provided. Scripts will be saved at path: {}", outputDirectory);
    }
    build(configuration, configDiff, typeMapping, templateDirectory, outputDirectory, stateDirectory, previousStateDirectory);
  }

  private static void validatePreviousStateDirectory(String previousStateDirectory) {
    if (previousStateDirectory == null || previousStateDirectory.equals("")) {
      throw new RuntimeException("--previous-state-directory path can not be null or empty.");
    }
    File f = new File(previousStateDirectory);
    if (!f.exists()) {
      log.info("--previous-state-directory does not exists.");
      StreamlinerUtil.createDir(previousStateDirectory);
      log.info("--previous-state-directory folder created. Path: {}", previousStateDirectory);
    }
  }

    private static void build(
      Configuration configuration,
      ConfigurationDiff configDiff,
      Map<String, Map<String, String>> typeMapping,
      String templateDirectory,
      String outputDirectory, String stateDirectory, String previousStateDirectory) {
    String pipeline =
        configDiff != null
            ? (configDiff.getPipeline() == null
                ? configuration.getPipeline()
                : configDiff.getPipeline())
            : configuration.getPipeline();
    List<File> files =
        StreamlinerUtil.listFilesInDir(String.format("%s/%s", templateDirectory, pipeline));
    List<File> templateFiles =
        files.stream().filter(f -> f.getName().endsWith(".ssp")).collect(Collectors.toList());
    List<File> nonTemplateFiles =
        files.stream().filter(f -> !f.getName().endsWith(".ssp")).collect(Collectors.toList());

    // if configDiff is found then evolve schema scripts are generated.
    if (configDiff != null) {
      log.info("streamliner-diff.yml file found.");
      if (configDiff.getTableDiffs() == null) {
        throw new RuntimeException(
            "TableDiff section is not found. Check the streamliner-diff.yml file in state-directory");
      }
      TemplateContext templateContext = new TemplateContext();
      List<TableDiff> tablesNotInSource =
          configDiff.getTableDiffs().stream()
              .filter(tableDiff -> !tableDiff.isExistsInSource())
              .collect(Collectors.toList());
      tablesNotInSource.forEach(
          table ->
              templateContext.addError(
                  String.format(
                      "Table %s.%s.%s does not exists in source schema. Currently Streamliner doesn't support table delete. User should delete the table manually from snowflake and config %s.yml from --previous-state-directory: %s",
                      ((Snowflake) configuration.getDestination()).getStagingDatabase().getName(),
                      ((Snowflake) configuration.getDestination()).getStagingDatabase().getSchema(),
                      table.getDestinationName(),
                      table.getDestinationName(),
                      previousStateDirectory)));

      List<TableDiff> tablesInSource =
          configDiff.getTableDiffs().stream()
              .filter(tableDiff -> tableDiff.isExistsInSource())
              .collect(Collectors.toList());
      // currently table delete is not supported.
      configDiff.setTableDiffs(tablesInSource);
      configDiff.getTableDiffs().stream()
          .forEach(
              tableDiff -> {
                String tableDiffDir =
                    String.format("%s/%s", outputDirectory, tableDiff.getDestinationName());
                StreamlinerUtil.createDir(tableDiffDir);
                templateFiles.stream()
                    .forEach(
                        templateFile -> {
                          log.info("Rendering file: {} ", templateFile);
                          Map<String, Object> map = new LinkedHashMap<>();
                          map.put("configurationDiff", configDiff);
                          map.put("table", getOriginalTable(configuration, tableDiff));
                          map.put("configuration", configuration);
                          map.put("tableDiff", tableDiff);
                          map.put("typeMapping", typeMapping);
                          map.put("templateContext", templateContext);
                          map.put("util", new StreamlinerUtil());
                          map.put(
                              "currConfigFile",
                              String.format(
                                  "%s/%s.yml", getAbsolutePath(stateDirectory), tableDiff.getDestinationName()));
                          map.put("prevStateDir", getAbsolutePath(previousStateDirectory));
                          String rendered = JavaHelper.getLayout(templateFile.getPath(), map);
                          String replaced = rendered.replace("    ", "\t");
                          log.debug(replaced);
                          String fileName =
                              String.format(
                                  "%s/%s",
                                  tableDiffDir, templateFile.getName().replace(".ssp", ""));
                          StreamlinerUtil.writeFile(replaced.trim(), fileName);
                          StreamlinerUtil.isExecutable(fileName);
                        });
              });
      writeSchemaMakeFile(
          configuration, configDiff, typeMapping, templateDirectory, outputDirectory);
      if (templateContext.hasErrors()) {
        List<String> errors = templateContext.getErrors();
        String msg =
            String.format("There are %d errors which require investigation.", errors.size());
        log.error(msg);
        errors.stream().forEach(error -> log.error(error));
        throw new RuntimeException(msg);
      }
    } else if (configuration != null) {
      log.info("Configuration (example: streamliner-configuration.yml) file found.");
      if (configuration.getTables() == null) {
        throw new RuntimeException(
            "Tables section is not found. Check the configuration (example: streamliner-configuration.yml) file");
      }

      TemplateContext templateContext = new TemplateContext();
      configuration.getTables().stream()
          .forEach(
              table -> {
                String tableDir =
                    String.format("%s/%s", outputDirectory, table.getDestinationName());
                StreamlinerUtil.createDir(tableDir);

                templateFiles.stream()
                    .forEach(
                        templateFile -> {
                          log.info("Rendering file: {} ", templateFile);
                          Map<String, Object> map = new LinkedHashMap<>();
                          map.put("configuration", configuration);
                          map.put("configurationDiff", new ConfigurationDiff());
                          map.put("tableDiff", new TableDiff());
                          map.put("table", table);
                          map.put("typeMapping", typeMapping);
                          map.put("templateContext", templateContext);
                          map.put("util", new StreamlinerUtil());
                          map.put(
                              "currConfigFile",
                              String.format(
                                  "%s/%s.yml", getAbsolutePath(stateDirectory), table.getSourceName()));
                          map.put("prevStateDir", getAbsolutePath(previousStateDirectory));
                          String rendered = JavaHelper.getLayout(templateFile.getPath(), map);

                          String replaced = rendered.replace("    ", "\t");
                          log.debug(replaced);
                          String fileName =
                              String.format(
                                  "%s/%s", tableDir, templateFile.getName().replace(".ssp", ""));
                          StreamlinerUtil.writeFile(replaced, fileName);
                          StreamlinerUtil.isExecutable(fileName);
                        });

                nonTemplateFiles.stream()
                    .forEach(
                        nonTemplateFile -> {
                          String content = StreamlinerUtil.readFile(nonTemplateFile.getPath());
                          String fileName =
                              String.format("%s/%s", tableDir, nonTemplateFile.getName());
                          StreamlinerUtil.writeFile(content, fileName);
                          StreamlinerUtil.isExecutable(fileName);
                        });
              });
      writeSchemaMakeFile(
          configuration, null, typeMapping, templateDirectory, outputDirectory);
      if (templateContext.hasErrors()) {
        List<String> errors = templateContext.getErrors();
        String msg =
            String.format("There are %d errors which require investigation.", errors.size());
        log.error(msg);
        errors.stream().forEach(error -> log.error(error));
        throw new RuntimeException(msg);
      }
    }
  }

  private static TableDefinition getOriginalTable(
      Configuration configuration, TableDiff tableDiff) {
    if (configuration.getTables() == null) {
      throw new RuntimeException(
          "Tables section is not found. Check the configuration (example: streamliner-configuration.yml) file");
    }
    List<TableDefinition> tableList =
        configuration.getTables().stream()
            .filter(
                table -> {
                  if (tableDiff.isExistsInSource()) {
                    if (table.getDestinationName().equals(tableDiff.getDestinationName())) {
                      return true;
                    }
                  }
                  return false;
                })
            .collect(Collectors.toList());
    return tableList.isEmpty()
        ? tableDiff.getType().equals("Snowflake") ? new SnowflakeTable() : new HadoopTable()
        : tableList.get(0);
  }

  private static void writeSchemaMakeFile(
      Configuration configuration,
      ConfigurationDiff configDiff,
      Map<String, Map<String, String>> typeMapping,
      String templateDirectory,
      String outputDirectory) {

    // Makefile.ssp support left in for backwards compatibility
    String makefile = String.format("%s/%s", templateDirectory, "Makefile.ssp");
    if (StreamlinerUtil.fileExists(makefile)) {
      log.info("Rendering Makefile: {} ", makefile);
      Map<String, Object> map = new LinkedHashMap<>();
      map.put("tables", configuration.getTables());
      map.put("configurationDiff", configDiff);
      map.put("typeMapping", typeMapping);
      String rendered = JavaHelper.getLayout(makefile, map);
      String replaced = rendered.replace("    ", "\t");
      log.debug(replaced);
      StreamlinerUtil.writeFile(replaced, outputDirectory + "/Makefile");
    }

    List<File> schemaFiles =
        StreamlinerUtil.listFilesInDir(templateDirectory).stream()
            .filter(file -> file.getName().contains(".schema"))
            .collect(Collectors.toList());
    List<File> schemaTemplateFiles =
        schemaFiles.stream().filter(f -> f.getName().endsWith(".ssp")).collect(Collectors.toList());

    schemaTemplateFiles.forEach(
        templateFile -> {
          log.info("Rendering file: {}", templateFile);
          Map<String, Object> map = new LinkedHashMap<>();
          map.put("configuration", configuration);
          map.put("tables", StreamlinerUtil.convertJavaListToScalaSeq(configuration.getTables()));
          map.put("configDiff", configDiff != null ? configDiff : new ConfigurationDiff());
          map.put("util", new StreamlinerUtil());
          map.put("typeMapping", typeMapping);
          String rendered = JavaHelper.getLayout(templateFile.getPath(), map);
          log.debug("Rendered file: {}", rendered);
          String templateFileName =
              templateFile.getName().replace(".ssp", "").replace(".schema", "");
          String fileName = String.format("%s/%s", outputDirectory, templateFileName);
          StreamlinerUtil.createDir(outputDirectory);
          StreamlinerUtil.writeFile(rendered, fileName);
          StreamlinerUtil.isExecutable(fileName);
        });
  }
  private static String getAbsolutePath(String path){
      File f = new File(path);
      return f.getAbsolutePath();
  }
}
