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

import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@EqualsAndHashCode(callSuper = false)
@ToString
@Getter
@Setter
public class ConfigurationDiff {
  private String name;
  private String environment;
  private String pipeline;
  private Destination previousDestination;
  public Destination currentDestination;
  private List<TableDiff> tableDiffs;

  public ConfigurationDiff() {}

  public ConfigurationDiff(
      String name,
      String environment,
      String pipeline,
      Destination previousDestination,
      Destination currentDestination,
      List<TableDiff> tableDiffs) {
    this.name = name;
    this.environment = environment;
    this.pipeline = pipeline;
    this.previousDestination = previousDestination;
    this.currentDestination = currentDestination;
    this.tableDiffs = tableDiffs;
  }
}
