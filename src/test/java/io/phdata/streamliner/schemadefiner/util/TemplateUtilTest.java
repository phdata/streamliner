package io.phdata.streamliner.schemadefiner.util;

import io.phdata.streamliner.schemadefiner.util.TemplateUtil;
import org.junit.Assert;
import org.junit.Test;

public class TemplateUtilTest {


    @Test
    public void testSnowflakeEscape()  {
        Assert.assertEquals("ABC", TemplateUtil.snowflakeEscape("ABC"));
        Assert.assertEquals("ABC''", TemplateUtil.snowflakeEscape("ABC'"));
        Assert.assertEquals("A''B''C''", TemplateUtil.snowflakeEscape("A'B'C'"));
        Assert.assertEquals("''", TemplateUtil.snowflakeEscape("'"));
        Assert.assertEquals("", TemplateUtil.snowflakeEscape(""));
        Assert.assertEquals(null, TemplateUtil.snowflakeEscape(null));
    }
}
