package io.phdata.streamliner.schemadefiner;

import io.phdata.streamliner.schemadefiner.model.Configuration;
import io.phdata.streamliner.schemadefiner.util.StreamlinerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import schemacrawler.crawl.SchemaDefinerHelper;
import schemacrawler.crawl.StreamlinerCatalog;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class StreamlinerConfigReader  implements SchemaDefiner{
    private static final Logger log = LoggerFactory.getLogger(StreamlinerConfigReader.class);
    private String file;
    private String configFilePath;

    public StreamlinerConfigReader(String configPath) {
        this.configFilePath = configPath;
    }

    @Override
    public StreamlinerCatalog retrieveSchema() throws IOException {
        log.info("Retrieving Schema from path: {}",configFilePath);
        if(!configPathExists(configFilePath)){
            throw new FileNotFoundException("Configuration File not found: " + configFilePath);
        }
        Configuration config = StreamlinerUtil.readConfigFromPath(configFilePath);
        return SchemaDefinerHelper.mapTableDefToStreamlinerCatalog(config);
    }

    private boolean configPathExists(String configPath) {
        return Files.exists(Paths.get(configPath));
    }
}
