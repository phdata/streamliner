package io.phdata.streamliner.schemadefiner.configbuilder;

import io.phdata.streamliner.schemadefiner.model.*;
import io.phdata.streamliner.schemadefiner.util.StreamlinerUtil;
import io.phdata.streamliner.util.JavaHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ScriptCommand {
  private static final Logger log = LoggerFactory.getLogger(ScriptCommand.class);

  public static void build(
      String configurationFile,
      String configurationDiffFile,
      String typeMappingFile,
      String templateDirectory,
      String outputDirectory) {

    Configuration configuration = StreamlinerUtil.readConfigFromPath(configurationFile);
    ConfigurationDiff configDiff = StreamlinerUtil.readConfigDiffFromPath(configurationDiffFile);
    if (configuration == null && configDiff == null) {
      throw new RuntimeException(
          "No configuration file found (example: streamliner-configuration.yml or streamliner-configuration-diff.yml) ");
    }
    if (configuration == null) {
      log.error("--config is mandatory");
      throw new RuntimeException(
          "--config is mandatory. Either no configuration file found (example: streamliner-configuration.yml) or file is empty. ");
    }
    Map<String, Map<String, String>> typeMapping =
        StreamlinerUtil.readTypeMappingFile(typeMappingFile);
    if (outputDirectory == null || outputDirectory.equals("")) {
      outputDirectory =
          String.format(
              "output/%s/%s/scripts", configuration.getName(), configuration.getEnvironment());
    }
    build(configuration, configDiff, typeMapping, templateDirectory, outputDirectory);
  }

  private static void build(
      Configuration configuration,
      ConfigurationDiff configDiff,
      Map<String, Map<String, String>> typeMapping,
      String templateDirectory,
      String outputDirectory) {
    String pipeline = configDiff != null ? configDiff.getPipeline() : configuration.getPipeline();
    List<File> files =
        StreamlinerUtil.listFilesInDir(String.format("%s/%s", templateDirectory, pipeline));
    List<File> templateFiles =
        files.stream().filter(f -> f.getName().endsWith(".ssp")).collect(Collectors.toList());
    List<File> nonTemplateFiles =
        files.stream().filter(f -> !f.getName().endsWith(".ssp")).collect(Collectors.toList());

    // if configDiff is found then evolve schema scripts are generated.
    if (configDiff != null) {
      log.info(
          "Configuration Difference (example: streamliner-configuration-diff.yml) file found.");
      if (configDiff.getTableDiffs() == null) {
        throw new RuntimeException(
            "TableDiff section is not found. Check the configuration difference (example: streamliner-configuration-diff.yml) file");
      }
      TemplateContext templateContext = new TemplateContext();
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
                          map.put("table", table);
                          map.put("typeMapping", typeMapping);
                          map.put("util", new StreamlinerUtil());
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
                  if (tableDiff.getExistsInSource()) {
                    if (table.getDestinationName().equals(tableDiff.getDestinationName())) {
                      return true;
                    }
                  }
                  return false;
                })
            .collect(Collectors.toList());
    return tableList.isEmpty() ? new TableDefinition() : tableList.get(0);
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
          map.put(
              "tableDiffs",
              StreamlinerUtil.convertJavaListToScalaSeq(
                  configDiff != null ? configDiff.getTableDiffs() : new ArrayList<>()));
          map.put("typeMapping", typeMapping);
          String rendered = JavaHelper.getLayout(templateFile.getPath(), map);
          log.debug("Rendered file: {}", rendered);
          String templateFileName =
              templateFile.getName().replace(".ssp", "").replace(".schema", "");
          String fileName = String.format("%s/%s", outputDirectory, templateFileName);
          StreamlinerUtil.writeFile(rendered, fileName);
          StreamlinerUtil.isExecutable(fileName);
        });
  }
}
