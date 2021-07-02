package io.phdata.streamliner.schemadefiner.ConfigBuilder;

import io.phdata.streamliner.schemadefiner.model.*;
import io.phdata.streamliner.schemadefiner.util.StreamlinerUtil;
import io.phdata.streamliner.util.JavaHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
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
      String outputDirectory)
      throws IOException {

    Configuration configuration = StreamlinerUtil.readConfigFromPath(configurationFile);
    ConfigurationDiff configDiff = StreamlinerUtil.readConfigDiffFromPath(configurationDiffFile);
    if (configuration == null && configDiff == null) {
      throw new RuntimeException(
          "No configuration file found (example: streamliner-configuration.yml or streamliner-configuration-diff.yml) ");
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
      String outputDirectory)
      throws IOException {
    String pipeline =
        configuration == null ? configDiff.getPipeline() : configuration.getPipeline();
    List<File> files =
        StreamlinerUtil.listFilesInDir(String.format("%s/%s", templateDirectory, pipeline));
    List<File> templateFiles =
        files.stream().filter(f -> f.getName().endsWith(".ssp")).collect(Collectors.toList());
    List<File> nonTemplateFiles =
        files.stream().filter(f -> !f.getName().endsWith(".ssp")).collect(Collectors.toList());

    if (configuration != null) {
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
      writeSchemaMakeFile(configuration, typeMapping, templateDirectory, outputDirectory);
    } else if (configDiff != null) {
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
      writeSchemaMakeFile(configDiff, templateDirectory, typeMapping, outputDirectory);
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

  private static void writeSchemaMakeFile(
      ConfigurationDiff configDiff,
      String templateDirectory,
      Map<String, Map<String, String>> typeMapping,
      String outputDirectory)
      throws FileNotFoundException {
    String makefile = String.format("%s/%s", templateDirectory, "Makefile.ssp");
    if (StreamlinerUtil.fileExists(makefile)) {
      log.info("Rendering schema-evolution Makefile: {} ", makefile);
      Map<String, Object> map = new LinkedHashMap<>();
      map.put("configurationDiff", configDiff);
      map.put("typeMapping", typeMapping);
      String rendered = JavaHelper.getLayout(makefile, map);
      String replaced = rendered.replace("    ", "\t");
      log.debug(replaced);
      StreamlinerUtil.writeFile(replaced, outputDirectory + "/Makefile");
    }

    List<File> schemaFiles =
        StreamlinerUtil.listFilesInDir(templateDirectory).stream()
            .filter(file -> file.getName().contains("evolve-schema.schema"))
            .collect(Collectors.toList());
    List<File> schemaTemplateFiles =
        schemaFiles.stream().filter(f -> f.getName().endsWith(".ssp")).collect(Collectors.toList());

    schemaTemplateFiles.forEach(
        templateFile -> {
          log.info("Rendering file: {}", templateFile);
          Map<String, Object> map = new LinkedHashMap<>();
          map.put(
              "tableDiffs", StreamlinerUtil.convertJavaListToScalaSeq(configDiff.getTableDiffs()));
          map.put("typeMapping", typeMapping);
          String rendered = JavaHelper.getLayout(templateFile.getPath(), map);
          log.debug("Rendered file: {}", rendered);
          String templateFileName =
              templateFile.getName().replace(".ssp", "").replace(".schema", "");
          String fileName = outputDirectory + "/" + templateFileName;
          StreamlinerUtil.writeFile(rendered, fileName);
          StreamlinerUtil.isExecutable(fileName);
        });
  }

  private static void writeSchemaMakeFile(
      Configuration configuration,
      Map<String, Map<String, String>> typeMapping,
      String templateDirectory,
      String outputDirectory)
      throws FileNotFoundException {

    // Makefile.ssp support left in for backwards compatibility
    String makefile = String.format("%s/%s", templateDirectory, "Makefile.ssp");
    if (StreamlinerUtil.fileExists(makefile)) {
      Map<String, Object> map = new LinkedHashMap<>();
      map.put("tables", configuration.getTables());
      String rendered = JavaHelper.getLayout(makefile, map);
      String replaced = rendered.replace("    ", "\t");
      log.debug(replaced);
      StreamlinerUtil.writeFile(replaced, outputDirectory + "/Makefile");
    }

    List<File> schemaFiles =
        StreamlinerUtil.listFilesInDir(templateDirectory).stream()
            .filter(
                file ->
                    file.getName().contains(".schema")
                        && !file.getName().contains("evolve-schema.schema"))
            .collect(Collectors.toList());
    List<File> schemaTemplateFiles =
        schemaFiles.stream().filter(f -> f.getName().endsWith(".ssp")).collect(Collectors.toList());

    schemaTemplateFiles.forEach(
        templateFile -> {
          log.info("Rendering file: {}", templateFile);
          Map<String, Object> map = new LinkedHashMap<>();
          map.put("configuration", configuration);
          map.put("tables", StreamlinerUtil.convertJavaListToScalaSeq(configuration.getTables()));
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
