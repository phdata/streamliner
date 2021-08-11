package io.phdata.streamliner.schemadefiner.model;

import io.phdata.streamliner.schemadefiner.util.StreamlinerUtil;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;

public class EnvSubstTest {
    @Rule
    public TemporaryFolder dir = new TemporaryFolder();

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
