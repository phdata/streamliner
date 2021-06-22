package io.phdata.streamliner.schemadefiner;

import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.AWSGlueClient;
import com.amazonaws.services.glue.model.GetTablesRequest;
import com.amazonaws.services.glue.model.GetTablesResult;
import com.amazonaws.services.glue.model.Table;
import io.phdata.streamliner.schemadefiner.model.FileFormat;
import io.phdata.streamliner.schemadefiner.model.GlueCatalog;
import schemacrawler.crawl.SchemaDefinerHelper;
import schemacrawler.crawl.StreamlinerCatalog;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GlueCrawler implements SchemaDefiner {

    private String database;
    private String region;
    private static Map<schemacrawler.schema.Table, FileFormat> tableFileFormatMap  = new HashMap<>();

    public GlueCrawler(GlueCatalog glueCatalog) {
        this.region = glueCatalog.getRegion();
        this.database = glueCatalog.getDatabase();
    }

    @Override
    public StreamlinerCatalog retrieveSchema() {
        List<Table> tableList = getTables(region, database);
        return SchemaDefinerHelper.mapAWSTableToCatalog(tableList, database , tableFileFormatMap);
    }

    private List<Table> getTables(String region, String database) {
        AWSGlue client = AWSGlueClient.builder().withRegion(region).build();
        GetTablesResult tables = client.getTables(new GetTablesRequest().withDatabaseName(database));
        return tables.getTableList();
    }

    public static Map<schemacrawler.schema.Table, FileFormat> getTableFileFormatMap() {
        return tableFileFormatMap;
    }
}
