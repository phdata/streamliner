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
package io.phdata.streamliner.schemadefiner.util;

import static org.junit.Assert.*;

import io.phdata.streamliner.schemadefiner.model.TableNameStrategy;
import org.junit.Test;

public class StreamlinerUtilTest {

  @Test
  public void applyTableNameStrategy_asIs_test() {
    String tableName = "EMPLOYEE";
    TableNameStrategy tableNameStrategy = new TableNameStrategy(null, null, true, null);
    String actual = StreamlinerUtil.applyTableNameStrategy(tableName, tableNameStrategy);
    assertEquals(tableName, actual);
  }

  @Test
  public void applyTableNameStrategy_addPostfix_test() {
    String tableName = "EMPLOYEE";
    String addPostfix = "_2";
    TableNameStrategy tableNameStrategy = new TableNameStrategy(null, addPostfix, false, null);
    String expected = "EMPLOYEE_2";
    String actual = StreamlinerUtil.applyTableNameStrategy(tableName, tableNameStrategy);
    assertEquals(expected, actual);
  }

  @Test
  public void applyTableNameStrategy_addPrefix_test() {
    String tableName = "EMPLOYEE";
    String addPrefix = "DEV_";
    TableNameStrategy tableNameStrategy = new TableNameStrategy(addPrefix, null, false, null);
    String expected = "DEV_EMPLOYEE";
    String actual = StreamlinerUtil.applyTableNameStrategy(tableName, tableNameStrategy);
    assertEquals(expected, actual);
  }

  @Test
  public void applyTableNameStrategy_removePrefix_test() {
    String tableName = "_DEV_EMPLOYEE_DEV";
    String search = "^_DEV";
    String replace = "";
    TableNameStrategy.SearchReplace searchReplace =
        new TableNameStrategy.SearchReplace(search, replace);
    TableNameStrategy tableNameStrategy = new TableNameStrategy(null, null, false, searchReplace);
    String expected = "_EMPLOYEE_DEV";
    String actual = StreamlinerUtil.applyTableNameStrategy(tableName, tableNameStrategy);
    assertEquals(expected, actual);
  }

  @Test
  public void applyTableNameStrategy_removePostfix_test() {
    String tableName = "_DEV_EMPLOYEE_DEV";
    String search = "_DEV$";
    String replace = "";
    TableNameStrategy.SearchReplace searchReplace =
        new TableNameStrategy.SearchReplace(search, replace);
    TableNameStrategy tableNameStrategy = new TableNameStrategy(null, null, false, searchReplace);
    String expected = "_DEV_EMPLOYEE";
    String actual = StreamlinerUtil.applyTableNameStrategy(tableName, tableNameStrategy);
    assertEquals(expected, actual);
  }

  @Test
  public void applyTableNameStrategy_removeAll_test() {
    String tableName = "_DEV_EMPLOYEE_DEV";
    String search = "DEV";
    String replace = "";
    TableNameStrategy.SearchReplace searchReplace =
        new TableNameStrategy.SearchReplace(search, replace);
    TableNameStrategy tableNameStrategy = new TableNameStrategy(null, null, false, searchReplace);
    String expected = "__EMPLOYEE_";
    String actual = StreamlinerUtil.applyTableNameStrategy(tableName, tableNameStrategy);
    assertEquals(expected, actual);
  }

  @Test
  public void applyTableNameStrategy_replacePrefix_test() {
    String tableName = "_DEV_EMPLOYEE_DEV";
    String search = "^_DEV";
    String replace = "PROD";
    TableNameStrategy.SearchReplace searchReplace =
        new TableNameStrategy.SearchReplace(search, replace);
    TableNameStrategy tableNameStrategy = new TableNameStrategy(null, null, false, searchReplace);
    String expected = "PROD_EMPLOYEE_DEV";
    String actual = StreamlinerUtil.applyTableNameStrategy(tableName, tableNameStrategy);
    assertEquals(expected, actual);
  }

  @Test
  public void applyTableNameStrategy_replacePostfix_test() {
    String tableName = "_DEV_EMPLOYEE_DEV";
    String search = "DEV$";
    String replace = "PROD";
    TableNameStrategy.SearchReplace searchReplace =
        new TableNameStrategy.SearchReplace(search, replace);
    TableNameStrategy tableNameStrategy = new TableNameStrategy(null, null, false, searchReplace);
    String expected = "_DEV_EMPLOYEE_PROD";
    String actual = StreamlinerUtil.applyTableNameStrategy(tableName, tableNameStrategy);
    assertEquals(expected, actual);
  }

  @Test
  public void applyTableNameStrategy_replaceAll_test() {
    String tableName = "_DEV_EMPLOYEE_DEV";
    String search = "DEV";
    String replace = "PROD";
    TableNameStrategy.SearchReplace searchReplace =
        new TableNameStrategy.SearchReplace(search, replace);
    TableNameStrategy tableNameStrategy = new TableNameStrategy(null, null, false, searchReplace);
    String expected = "_PROD_EMPLOYEE_PROD";
    String actual = StreamlinerUtil.applyTableNameStrategy(tableName, tableNameStrategy);
    assertEquals(expected, actual);
  }

  @Test
  public void applyTableNameStrategy_replace1stOccurrence_test() {
    String tableName = "_DEV_EMPLOYEE_DEV_COUNTRY_DEV";
    String search = "DEV";
    String replace = "PROD";
    Integer occurrences[] = {1};
    TableNameStrategy.SearchReplace searchReplace =
        new TableNameStrategy.SearchReplace(search, replace, occurrences);
    TableNameStrategy tableNameStrategy = new TableNameStrategy(null, null, false, searchReplace);
    String expected = "_PROD_EMPLOYEE_DEV_COUNTRY_DEV";
    String actual = StreamlinerUtil.applyTableNameStrategy(tableName, tableNameStrategy);
    assertEquals(expected, actual);
  }

  @Test
  public void applyTableNameStrategy_replace2ndOccurrence_test() {
    String tableName = "_DEV_EMPLOYEE_DEV_COUNTRY_DEV";
    String search = "DEV";
    String replace = "PROD";
    Integer occurrences[] = {2};
    TableNameStrategy.SearchReplace searchReplace =
        new TableNameStrategy.SearchReplace(search, replace, occurrences);
    TableNameStrategy tableNameStrategy = new TableNameStrategy(null, null, false, searchReplace);
    String expected = "_DEV_EMPLOYEE_PROD_COUNTRY_DEV";
    String actual = StreamlinerUtil.applyTableNameStrategy(tableName, tableNameStrategy);
    assertEquals(expected, actual);
  }

  @Test
  public void applyTableNameStrategy_replace1stAnd3rdOccurrence_test() {
    String tableName = "_DEV_EMPLOYEE_DEV_COUNTRY_DEV";
    String search = "DEV";
    String replace = "PROD";
    Integer occurrences[] = {1, 3};
    TableNameStrategy.SearchReplace searchReplace =
        new TableNameStrategy.SearchReplace(search, replace, occurrences);
    TableNameStrategy tableNameStrategy = new TableNameStrategy(null, null, false, searchReplace);
    String expected = "_PROD_EMPLOYEE_DEV_COUNTRY_PROD";
    String actual = StreamlinerUtil.applyTableNameStrategy(tableName, tableNameStrategy);
    assertEquals(expected, actual);
  }
}
