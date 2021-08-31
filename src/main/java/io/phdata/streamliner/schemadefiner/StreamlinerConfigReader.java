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
public class StreamlinerConfigReader  implements SchemaDefiner{
    private static final Logger log = LoggerFactory.getLogger(StreamlinerConfigReader.class);
    private String configFilePath;

    public StreamlinerConfigReader(String configPath) {
        this.configFilePath = configPath;
    }

    @Override
    public StreamlinerCatalog retrieveSchema() {
        log.info("Retrieving Schema from path: {}",configFilePath);
        if(!StreamlinerUtil.fileExists(configFilePath)){
            throw new RuntimeException(String.format("Configuration File not found: %s", configFilePath));
        }
        Configuration config = StreamlinerUtil.readConfigFromPath(configFilePath);
        return SchemaDefinerHelper.mapTableDefToStreamlinerCatalog(config);
    }


}
