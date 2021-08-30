package io.phdata.streamliner.schemadefiner.util;

public class TemplateUtil {

    public static String snowflakeEscape(String s) {
        if (s == null) return null;
        return s.replace("'", "''");
    }
}
