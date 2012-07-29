package edu.washington.cs.knowitall.browser.lucene

import scala.sys.process._
import java.io.File
import java.io.FileInputStream
import edu.washington.cs.knowitall.common.Resource.using
import edu.washington.cs.knowitall.common.Timing
import edu.washington.cs.knowitall.browser.extraction.ReVerbExtraction

import edu.washington.cs.knowitall.tool.tokenize.OpenNlpTokenizer

/**
 * Examines the differences between the files in a local dir and an hdfs dir. Files not in
 * the hdfs dir are both added to the hdfs dir, and also sent to ParallelReVerbIndexModifier
 */
class Ingester(
    val indexModifier: IndexModifier,
    val converterJar: String, 
    val hadoopDir: String, 
    val localDir: String, 
    val localDirHost: String, 
    val sshIdentityKeyFile: String,
    val corpus: String) {
  
  import Ingester.printErr
  import ParallelReVerbIndexModifier.extrToSingletonGroup
  
  private var extractionsIngested = 0
  
  private val whiteSpaceRegex = "\\s+".r
  
  private def stripPathAndExt(fileName: String): String = {
    val noPath = fileName.drop(fileName.lastIndexOf(File.separatorChar + 1))
      val noExt = noPath.take(noPath.indexOf("""."""))
      noExt
  }
  
  private def filesInHadoopDir: Set[String] = {
    
    val fullPathNames = Process("hadoop dfs -ls %s".format(hadoopDir)).lines.flatMap { cmdOutputLine => 
      val split = whiteSpaceRegex.split(cmdOutputLine)
      split match {
        case Array(attrs, repl, owner, group, size, dateYmd, time, fullName, _*) => {
          if (size.toInt > 0) Some(fullName) else None
        }
        case _ => None
      }
    }

    fullPathNames map stripPathAndExt toSet
  }
  
  private def filesInLocalDir: Map[String, File] = {
    
    val cmd = "ssh -i %s %s ls -1 %s".format(sshIdentityKeyFile, localDirHost, localDir)
    printErr("Executing command: %s".format(cmd))
    Process(cmd).lines map { fileName => (stripPathAndExt(fileName), new File(fileName)) } toMap
  }
  
  private def filesNotInHadoop: Map[String, File] = filesInLocalDir -- filesInHadoopDir
  
  private def extrFilter(extr: ReVerbExtraction): Boolean = {
    try {
      printErr("(%s) %s".format(extr.indexGroupingKeyString, extr.toString))
      true
    } catch {
      case e: Exception => { e.printStackTrace; false }
    }
  }
  
  private def ingestHdfsToIndex(hdfsFile: String): Unit = {
    val hdfsCmd = "hadoop dfs -cat %s".format(hdfsFile)
    printErr("Executing command: %s".format(hdfsCmd))
    val extrs = (Process(hdfsCmd) #| Process("lzop -cd")).lines.iterator flatMap ReVerbExtraction.deserializeFromString
    val groups = extrs filter extrFilter map extrToSingletonGroup(corpus) toSeq;
    extractionsIngested += groups.size
    indexModifier.updateAll(groups.iterator)
  }
  
  private def ingestFileToHdfs(file: File): String = {
    val hadoopFile = "%s/%s".format(hadoopDir, file.getName + ".lzo")
    val remoteCatCmd = "ssh -i %s %s cat %s/%s".format(sshIdentityKeyFile, localDirHost, localDir, file.getName)
    remoteCatCmd #> "java -jar %s".format(converterJar) #| "lzop -c" #| "hadoop dfs -put - %s".format(hadoopFile) !
    
    hadoopFile
  }

  def run: Unit = {
    val filesToIngest = filesNotInHadoop.iterator.toSeq
    printErr("Ingesting %d files".format(filesToIngest.size))
    filesToIngest foreach { case (name, file) => 
      printErr("Ingesting %s into HDFS".format(name))
      val hadoopFile = ingestFileToHdfs(file)
      printErr("Ingesting %s into index".format(name))
      ingestHdfsToIndex(hadoopFile)
    }
    printErr("Ingestion complete, %d extractions total extractions ingested from %d files".format(extractionsIngested, filesToIngest.size) )
  }
}

object Ingester {
  
  private def printErr(line: String): Unit = System.err.println(line)
  
  def main(args: Array[String]): Unit = {
    
    var indexPaths: Seq[String] = Nil
    var ramBufferMb: Int = 500
    var linesPerCommit: Int = 25000
    var converterJar: String = ""
    var hadoopDir: String = ""
    var localDir: String = ""
    var localDirHost: String = "" 
    var sshIdentityKeyFile: String = ""
    var corpus: String = ""
      
    val parser = new scopt.OptionParser() {
      arg("indexPaths", "Path to parallel lucene indexes, colon separated", { str => indexPaths = str.split(":") })
      arg("converterJar", "full path to David Jung's ReVerb format converter jar", { converterJar = _ })
      arg("localDirHost", "hostname where JSON ReVerb is located, e.g. rv-n14", { localDirHost = _ })
      arg("localDir", "directory on localDirHost where JSON data is located", { localDir = _ })
      arg("hadoopDir", "hdfs directory for converted reverb data", { hadoopDir = _ })
      arg("sshKey", "ssh identity to use for ssh-ing to localDirHost", { sshIdentityKeyFile = _ })
      arg("corpus", "corpus identifier to attach to new data in the index", { corpus = _ })
      intOpt("ramBufferMb", "ramBuffer in MB per index", { ramBufferMb = _ })
      intOpt("linesPerCommit", "num lines between index commits", { linesPerCommit = _ })
    }
    
    if (!parser.parse(args)) return else printErr("Parsed args: %s".format(args.mkString(", ")))
    
    val indexModifier = new ParallelReVerbIndexModifier(
      indexPaths=indexPaths,
      ramBufferMb=ramBufferMb,
      linesPerCommit=linesPerCommit
    )
    
    val ingester = new Ingester(
      indexModifier=indexModifier,
      converterJar=converterJar,
      hadoopDir=hadoopDir,
      localDir=localDir,
      localDirHost=localDirHost,
      sshIdentityKeyFile=sshIdentityKeyFile,
      corpus=corpus
    )
    
    val nsRuntime = Timing.time {
      ingester.run
      indexModifier.close
    }
    println("Run time: %s".format(Timing.Seconds.format(nsRuntime)))
  }
}