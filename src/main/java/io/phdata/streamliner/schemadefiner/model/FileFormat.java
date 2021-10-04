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
public class FileFormat {
  private String location;
  private String fileType;
  private String delimiter;
  private String nullIf;

  public FileFormat() {}

  public FileFormat(String location, String fileType) {
    this.location = location;
    this.fileType = fileType;
  }

  public FileFormat(String location, String fileType, String delimiter, String nullIf) {
    this.location = location;
    this.fileType = fileType;
    this.delimiter = delimiter;
    this.nullIf = nullIf;
  }
}
