package io.phdata.streamliner.schemadefiner.model;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class ColumnDefinitionTest {

    @Test
    public void testLongColumnType(){
        ColumnDefinition c = new ColumnDefinition("c1", "c1", "VARCHAR", null,
                17_777_216, null, false);
        Map<String, Map<String, String>> typeMap = new HashMap<>();
        Map<String, String> subMap = new HashMap<>();
        subMap.put("snowflake", "varchar");
        typeMap.put("varchar", subMap);
        String result = c.mapDataTypeSnowflake(typeMap);
        Assert.assertEquals("VARCHAR(16777216)", result);
    }
}
