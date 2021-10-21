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
package io.phdata.streamliner.schemadefiner.model;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@EqualsAndHashCode(callSuper = false)
@ToString
@Getter
@Setter
public class Configuration {
  public String name;
  public String environment;
  private String pipeline;
  public Source source;
  public Destination destination;
  private List<TableDefinition> tables;
  /* This can be used to pass any other extra fields needed. */
  public Map<Object, Object> genericProperties;

  public Configuration() {}

  public Configuration(
      String name,
      String environment,
      String pipeline,
      Source source,
      Destination destination,
      Map<Object, Object> genericProperties,
      List<TableDefinition> tables) {
    this.name = name;
    this.environment = environment;
    this.pipeline = pipeline;
    this.source = source;
    this.destination = destination;
    this.genericProperties = genericProperties;
    this.tables = tables;
  }

  public Configuration(List<TableDefinition> tables) {
    this.tables = tables;
  }

  public Configuration(
      String name,
      String environment,
      String pipeline,
      Source source,
      Destination destination,
      Map<Object, Object> genericProperties) {
    this.name = name;
    this.environment = environment;
    this.pipeline = pipeline;
    this.source = source;
    this.destination = destination;
    this.genericProperties = genericProperties;
  }

  public void addTableDefinition(TableDefinition table) {
    if (this.tables == null) {
      this.tables = new ArrayList<>();
      this.tables.add(table);
    } else {
      this.tables.add(table);
    }
  }
}
