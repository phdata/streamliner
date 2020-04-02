/*
 * Copyright 2018 phData Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.phdata.pipewrench.util

import java.io.{File, FileNotFoundException, FileWriter}
import java.nio.file.Files
import java.nio.file.Paths

import org.slf4j.LoggerFactory

import scala.io.Source

trait FileUtil {

  private val logger = LoggerFactory.getLogger("FileUtil")

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
      throw new FileNotFoundException(s"Directory path: $path does not exist")
    }
  }

  def createDir(path: String): Unit = {
    val f = new File(path)
    if (!f.exists()) {
      logger.debug(s"Creating directory: $path")
      f.mkdirs()
    }
  }

  def fileExists(path: String) = {
    Files.exists(Paths.get(path))
  }
}
