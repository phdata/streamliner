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

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@EqualsAndHashCode(callSuper = false)
@ToString
@Getter
@Setter
public class TableNameStrategy {
  private String addPrefix;
  private String addPostfix;
  private boolean asIs;
  private SearchReplace searchReplace;

  public TableNameStrategy(
      String addPrefix, String addPostfix, boolean asIs, SearchReplace searchReplace) {
    this.addPrefix = addPrefix;
    this.addPostfix = addPostfix;
    this.asIs = asIs;
    this.searchReplace = searchReplace;
  }

  public TableNameStrategy() {}

  /* search: regex can be used to find a string
  * replace: This string will be the replacement at searched string
  * occurrences: Array of Integer can be provided. If this is used the searched string will replaced only at provided occurrence values
  *
  * Example:
  * 1. Use of occurrences
  *   tableName: "_DEV_EMPLOYEE_DEV_COUNTRY_DEV"
  *   search: "DEV"
  *   replace: "PROD"
  *   occurrences: [1,3]
  *   output: "_PROD_EMPLOYEE_DEV_COUNTRY_PROD"
  *
  * 2. Remove prefix
      tableName: "_DEV_EMPLOYEE_DEV"
  *   search: "^_DEV"
  *   replace: ""
  *   output: "_EMPLOYEE_DEV"
  * */
  @EqualsAndHashCode(callSuper = false)
  @ToString
  @Getter
  @Setter
  public static class SearchReplace {
    private String search;
    private String replace;
    private Integer[] occurrences;

    public SearchReplace(String search, String replace) {
      this.search = search;
      this.replace = replace;
    }

    public SearchReplace(String search, String replace, Integer[] occurrences) {
      this.search = search;
      this.replace = replace;
      this.occurrences = occurrences;
    }

    public SearchReplace() {}
  }
}
