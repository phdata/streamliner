package io.phdata.streamliner.util

import org.fusesource.scalate.TemplateEngine

import java.util
import scala.collection.JavaConverters._;

object JavaHelper {
  private lazy val engine: TemplateEngine = new TemplateEngine

  engine.escapeMarkup = true;

  def getLayout(uri: String, attributes: java.util.Map[String, Object]): String = {
    val typeMapping = attributes.get("typeMapping").asInstanceOf[util.Map[String, Map[String, String]]];
    val keys = typeMapping.keySet()
    val temp = new util.LinkedHashMap[String, Map[String, String]]()
    keys.forEach(k => temp.put(k, typeMapping.get(k).asInstanceOf[util.Map[String, String]].asScala.toMap))
    val scalaTypeMapping = temp.asScala.toMap
    attributes.put("typeMapping", scalaTypeMapping)
    engine.layout(uri, attributes.asScala.toMap)
  }

  def convertJavaMapToScalaMap(map: java.util.Map[String, String]): Map[String, String] = {
    map.asScala.toMap
  }

  def convertScalaMapToJavaMap(map: Map[String, Map[String, String]]): util.Map[String, util.Map[String, String]] = {
    val keys = map.keySet
    val newMap = new util.LinkedHashMap[String, util.Map[String, String]]()
    keys.foreach(k => {
      val temp = map.get(k).get
      newMap.put(k, temp.asJava)
    })
    newMap
  }

}
