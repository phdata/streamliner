package io.phdata.streamliner.schemadefiner;

import schemacrawler.crawl.StreamlinerCatalog;

public interface SchemaDefiner {

 StreamlinerCatalog retrieveSchema();
}
