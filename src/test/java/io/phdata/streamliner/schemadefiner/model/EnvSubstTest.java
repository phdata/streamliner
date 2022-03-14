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

import io.phdata.streamliner.schemadefiner.util.StreamlinerUtil;
import java.io.File;
import java.io.IOException;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class EnvSubstTest {
  @Rule public TemporaryFolder dir = new TemporaryFolder();

  @Test
  public void testEnv() throws IOException {
    Configuration c1 = new Configuration();
    c1.setName("${env:PATH}");
    File yamlFile = dir.newFile();
    StreamlinerUtil.writeYamlFile(c1, yamlFile.getAbsolutePath());
    Configuration c2 = StreamlinerUtil.readYamlFile(yamlFile.getAbsolutePath());
    Assert.assertEquals(System.getenv("PATH"), c2.getName());
  }

  @Test
  public void testEnv2() throws IOException {
    Configuration c1 = new Configuration();
    c1.setName("Testing --path=${env:PATH}");
    File yamlFile = dir.newFile();
    StreamlinerUtil.writeYamlFile(c1, yamlFile.getAbsolutePath());
    Configuration c2 = StreamlinerUtil.readYamlFile(yamlFile.getAbsolutePath());
    Assert.assertEquals(String.format("Testing --path=%s", System.getenv("PATH")), c2.getName());
  }

  @Test
  public void testNoReplacementWithoutEnv1() throws IOException {
    Configuration c1 = new Configuration();
    c1.setName("${PATH}");
    File yamlFile = dir.newFile();
    StreamlinerUtil.writeYamlFile(c1, yamlFile.getAbsolutePath());
    Configuration c2 = StreamlinerUtil.readYamlFile(yamlFile.getAbsolutePath());
    Assert.assertEquals("${PATH}", c2.getName());
  }

  @Test
  public void testNoReplacementWithoutEnv2() throws IOException {
    Configuration c1 = new Configuration();
    c1.setName("$PATH");
    File yamlFile = dir.newFile();
    StreamlinerUtil.writeYamlFile(c1, yamlFile.getAbsolutePath());
    Configuration c2 = StreamlinerUtil.readYamlFile(yamlFile.getAbsolutePath());
    Assert.assertEquals("$PATH", c2.getName());
  }
}
