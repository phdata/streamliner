package io.phdata.streamliner.schemadefiner.configbuilder;

import io.phdata.streamliner.schemadefiner.model.*;
import io.phdata.streamliner.schemadefiner.util.StreamlinerUtil;
import io.phdata.streamliner.util.JavaHelper;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.*;
import java.util.stream.Collectors;

public class ScriptCommand {
  private static final Logger log = LoggerFactory.getLogger(ScriptCommand.class);
  private Map<String, Map<String, List>> commonTables = new LinkedHashMap<>();;
  private Map<String, Collection> addedOrDeletedorIncompatibleTables = new LinkedHashMap<>();;

    // config is ingest-configuration.yml
  public void build(
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
    log.info("Scripts generated successfully.");
  }

  private void validatePreviousStateDirectory(String previousStateDirectory) {
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

    private void build(
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
      addedOrDeletedorIncompatibleTables.put(
          Constants.TABLES_DELETED.value(),
          tablesNotInSource.stream()
              .map(table -> table.getDestinationName())
              .collect(Collectors.toList()));
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

      try {
        /*intentionally kept inside try catch block.
        any exception while generating the delta-change-summary should not block the run. */
        if (configDiff.getTableDiffs() != null && !configDiff.getTableDiffs().isEmpty()) {
          generateDeltaChangeSummary(configDiff, configuration, typeMapping, outputDirectory);
        }
      } catch (Exception e) {
        String errorMsg = String.format("Error generating delta-change-summary file. %s", e);
        log.error(errorMsg);
        StreamlinerUtil.writeFile(
            errorMsg, String.format("%s/%s", outputDirectory, "delta-change-summary.txt"));
      }
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

  private void generateDeltaChangeSummary(
      ConfigurationDiff configDiff,
      Configuration config,
      Map<String, Map<String, String>> typeMapping,
      String outputDirectory) {
    // this does not have deleted tables. deleted tables are already removed from this list.
    Set<TableDiff> tableDiffs = new LinkedHashSet<>(configDiff.getTableDiffs());
    Set<SchemaChanges> validSchemaChanges = getValidSchemaChanges(config.getSource());
    if (validSchemaChanges == null || validSchemaChanges.isEmpty()) {
      validSchemaChanges.addAll(Arrays.asList(SchemaChanges.values()));
    }
    List<TableDiff> addedTables =
        tableDiffs.stream()
            .filter(tableDiff -> tableDiff.existsInSource && !tableDiff.isExistsInDestination())
            .collect(Collectors.toList());
    List<String> addedTablesName =
        addedTables.stream()
            .map(tableDiff -> tableDiff.getDestinationName())
            .collect(Collectors.toList());
    addedOrDeletedorIncompatibleTables.put(Constants.TABLES_ADDED.value(), addedTablesName);
    Set<String> incompatibleTables =
        tableDiffs.stream()
            .filter(
                tableDiff ->
                    !tableDiff.allChangesAreCompatible(
                        JavaHelper.convertJavaMapToScalaMap(typeMapping), validSchemaChanges))
            .map(incompatibleTable -> incompatibleTable.getDestinationName())
            .collect(Collectors.toSet());
    //Deleted tables are also added as incompatible change tables.
    incompatibleTables.addAll(addedOrDeletedorIncompatibleTables.get(Constants.TABLES_DELETED.value()));
    addedOrDeletedorIncompatibleTables.put(
        Constants.TABLES_INCOMPATIBLE.value(), incompatibleTables);

    // now tableDiffs will have updatedTables only.
    tableDiffs.removeAll(addedTables);

    tableDiffs.stream()
        .forEach(
            tableDiff -> {
              List<ColumnDiff> columnDiffs = tableDiff.getColumnDiffs();
              List<String> columnsAdded =
                  columnDiffs.stream()
                      .filter(columnDiff -> columnDiff.getIsAdd())
                      .map(col -> col.getCurrentColumnDef().getSourceName())
                      .collect(Collectors.toList());
              List<String> columnsDeleted =
                  columnDiffs.stream()
                      .filter(columnDiff -> columnDiff.getIsDeleted())
                      .map(col -> col.getPreviousColumnDef().getSourceName())
                      .collect(Collectors.toList());
              List<ColumnDiff> columnsUpdated =
                  columnDiffs.stream()
                      .filter(columnDiff -> columnDiff.getIsUpdate())
                      .collect(Collectors.toList());
              Map<String, List> columnsDetail = new LinkedHashMap<>();
              columnsDetail.put(Constants.COLUMNS_UPDATED.value(), columnsUpdated);
              columnsDetail.put(Constants.COLUMNS_DELETED.value(), columnsDeleted);
              columnsDetail.put(Constants.COLUMNS_ADDED.value(), columnsAdded);
              commonTables.put(tableDiff.getDestinationName(), columnsDetail);
            });

    createContent(validSchemaChanges, outputDirectory, typeMapping);
  }

  private Set<SchemaChanges> getValidSchemaChanges(Source source) {
    if (source instanceof Jdbc) {
      return ((Jdbc) source).getValidSchemaChanges();
    }
    return null;
  }

  private void createContent(
      Set<SchemaChanges> validSchemaChanges,
      String outputDirectory,
      Map<String, Map<String, String>> typeMapping) {
    StringBuilder sb = new StringBuilder();
    sb.append("\nSTREAMLINER DELTA CHANGE SUMMARY\n");
    sb.append("***********************************************************\n");
    sb.append(String.format("ALLOWED SCHEMA CHANGES: %s\n\n", validSchemaChanges.toString()));
    sb.append(
        String.format(
            "TABLES ADDED: %s\n",
            addedOrDeletedorIncompatibleTables.get(Constants.TABLES_ADDED.value()).toString()));
    sb.append(
        String.format(
            "TABLES DELETED: %s\n",
            addedOrDeletedorIncompatibleTables.get(Constants.TABLES_DELETED.value()).toString()));

    if (commonTables.isEmpty()) {
      sb.append("TABLES UPDATED: []\n");
    } else {
      sb.append("TABLES UPDATED:\n");
      sb.append("===============\n");
      for (Map.Entry<String, Map<String, List>> entry : commonTables.entrySet()) {
        Map<String, List> colDetail = entry.getValue();
        sb.append(String.format("TABLE: %s\n", entry.getKey()));
        sb.append("------------------------------\n");
        sb.append(
            String.format("COLUMN ADDED: %s\n", colDetail.get(Constants.COLUMNS_ADDED.value())));
        sb.append(
            String.format(
                "COLUMN DELETED: %s\n", colDetail.get(Constants.COLUMNS_DELETED.value())));
        List<ColumnDiff> colDiffs = colDetail.get(Constants.COLUMNS_UPDATED.value());
        TableDiff tableDiff = new TableDiff();
        if (colDiffs.isEmpty()) {
          sb.append("COLUMN UPDATED: []\n");
        } else {
          sb.append("COLUMN UPDATED:\n");
        }
        colDiffs.forEach(
            colDiff -> {
              List<String> changes = new ArrayList<>();
              ColumnDefinition currDef = colDiff.getCurrentColumnDef();
              ColumnDefinition prevDef = colDiff.getPreviousColumnDef();
              if (!tableDiff.isColumnCommentSame(currDef, prevDef)) {
                changes.add("COMMENT");
              }
              if (!tableDiff.isColumnNullableSame(currDef, prevDef)) {
                changes.add("NULLABILITY");
              }
              if (tableDiff.isPrecisionChangeValid(currDef, prevDef, typeMapping)) {
                changes.add("LENGTH");
              }
              if (!tableDiff.isDataTypeSame(currDef, prevDef)) {
                changes.add("DATA TYPE");
              }
              sb.append(
                  String.format(
                      "%s -- %s\n", currDef.getDestinationName(), StringUtils.join(changes, ", ")));
            });
        sb.append("------------------------------\n\n");
      }
    }
    sb.append("\n");
    sb.append(
        String.format(
            "TABLES WITH INCOMPATIBLE CHANGES: %s\n",
            addedOrDeletedorIncompatibleTables
                .get(Constants.TABLES_INCOMPATIBLE.value())
                .toString()));
    StreamlinerUtil.writeFile(
        sb.toString(), String.format("%s/%s", outputDirectory, "delta-change-summary.txt"));
  }

    private TableDefinition getOriginalTable(
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

  private void writeSchemaMakeFile(
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
          String templateFileName =
              templateFile.getName().replace(".ssp", "").replace(".schema", "");
          String fileName = String.format("%s/%s", outputDirectory, templateFileName);
          StreamlinerUtil.createDir(outputDirectory);
          StreamlinerUtil.writeFile(rendered, fileName);
          StreamlinerUtil.isExecutable(fileName);
        });
  }
  private String getAbsolutePath(String path){
      File f = new File(path);
      return f.getAbsolutePath();
  }
}
