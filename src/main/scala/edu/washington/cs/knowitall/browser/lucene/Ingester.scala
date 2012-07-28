package edu.washington.cs.knowitall.browser.lucene

import scala.sys.process._

/**
 * Examines the differences between the files in a local dir and an hdfs dir. Files not in
 * the hdfs dir are both added to the hdfs dir, and also sent to ParallelReVerbIndexModifier
 */
class Ingester(val hadoopDir: String, val localDir: String) {
  
  private val whiteSpaceRegex = "\\s+".r
  
  private def filesInHadoopDir: Seq[String] = {
    
    Process("hadoop dfs -ls %s".format(hadoopDir)).lines.flatMap { cmdOutputLine => 
      val split = whiteSpaceRegex.split(cmdOutputLine)
      split match {
        case Array(something, _*) => Some("not implemented")
        case _ => Some("not implemented either")
      }
    }
  }
}
