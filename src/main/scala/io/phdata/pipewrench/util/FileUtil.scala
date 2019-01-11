package io.phdata.pipewrench.util

import java.io.{File, FileWriter}

import com.typesafe.scalalogging.LazyLogging
import scala.io.Source

trait FileUtil extends LazyLogging {

  def readFile(path: String): String = {
    logger.debug(s"Reading file: $path")
    Source.fromFile(path).getLines().mkString("\n")
  }

  def setExecutable(path: String): Unit = {
    val f = new File(path)
    f.setExecutable(true)
  }

  def writeFile(content: String, path: String): Unit = {
    logger.info(s"Writing file: $path")
    logger.debug(s"File content: $content")
    val fw = new FileWriter(path)
    fw.write(content)
    fw.close()
  }

  def listFilesInDir(path: String): List[File] = {
    val d = new File(path)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }

  def createDir(path: String): Unit = {
    val f = new File(path)
    if (!f.exists()) {
      logger.debug(s"Creating directory: $path")
      f.mkdirs()
    }
  }
}
