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
package io.phdata.streamliner.schemadefiner.attributeretriever;

import java.sql.Connection;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AttributeRetrieverFactory {

  private static final Logger log = LoggerFactory.getLogger(AttributeRetrieverFactory.class);

  public static AttributeRetriever getAttributeRetriever(
      String dbType, final Supplier<Connection> connectionSupplier) {
    switch (dbType) {
      case "hive":
        return new HiveAttributeRetriever(connectionSupplier.get());
      case "hive2":
        return new HiveAttributeRetriever(connectionSupplier.get());
      case "impala":
        return new HiveAttributeRetriever(connectionSupplier.get());
      default:
        return new NoOpAttributeRetriever(dbType);
    }
  }
}
