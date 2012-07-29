package edu.washington.cs.knowitall.browser.lucene

import edu.washington.cs.knowitall.browser.hadoop.scoobi.ScoobiEntityLinker
import edu.washington.cs.knowitall.browser.hadoop.scoobi.ScoobiGroupReGrouper

import edu.washington.cs.knowitall.browser.extraction.ExtractionArgument
import edu.washington.cs.knowitall.browser.extraction.ExtractionRelation
import edu.washington.cs.knowitall.browser.extraction.ExtractionGroup
import edu.washington.cs.knowitall.browser.extraction.Instance
import edu.washington.cs.knowitall.browser.extraction.ExtractionGroup
import edu.washington.cs.knowitall.browser.extraction.ReVerbExtractionGroup
import edu.washington.cs.knowitall.browser.extraction.ReVerbExtraction

import org.apache.lucene.index.IndexWriter
import org.apache.lucene.store.FSDirectory

import java.io.File

import scopt.OptionParser

import scala.io.Source

class ParallelReVerbIndexModifier(val basicModifiers: Seq[ReVerbIndexModifier], groupsPerCommit: Int) extends IndexModifier {

  import ParallelReVerbIndexModifier.loadSubModifier
  
  def this(indexPaths: Seq[String], ramBufferMb: Int, linesPerCommit: Int) = {
    this(indexPaths map ParallelReVerbIndexModifier.loadSubModifier(linesPerCommit, ramBufferMb), linesPerCommit)
  }
  
  def fetcher = new ParallelExtractionGroupFetcher(basicModifiers.map(_.fetcher))

  private def updateGroup(group: REG): Boolean = {

    var exception: Option[Exception] = None

    val updated = basicModifiers.par.map { modifier =>
      try {
        modifier.updateGroup(group, onlyIfAlreadyExists = true)
      } catch {
        case e: Exception => exception = Some(e)
      }

    } exists (_ == true)

    exception match {
      case Some(e) => throw e
      case None => updated
    }
  }
  
  private def addToRandomGroup(group: REG): Unit = {
    
    val randomModifier = basicModifiers(scala.util.Random.nextInt(basicModifiers.length))
    randomModifier.addGroup(group, true)
  }
  
  def updateAll(groups: Iterator[REG]): Unit = {
    
    var groupsProcessed = 0
    var exceptions = 0
    groups.grouped(groupsPerCommit).foreach { groupOfGroups =>
      
      groupOfGroups.foreach { group => 
        try {
          val updated = updateGroup(group)
          if (!updated) addToRandomGroup(group)
        } catch {
          case e: Exception => { exceptions += 1; e.printStackTrace }
        }
      }
      
      groupsProcessed += groupOfGroups.size
      basicModifiers map(_.writer.commit())
      System.err.println("Groups inserted: %d, Exceptions: %d".format(groupsProcessed, exceptions))
    }
  }
  
  def close(): Unit = {
    basicModifiers.foreach { _.close() }
  }
}

object ParallelReVerbIndexModifier {
  
  def loadSubModifier(linesPerCommit: Int, ramBufferMb: Int)(indexPath: String): ReVerbIndexModifier = {
    val indexWriter = new IndexWriter(FSDirectory.open(new File(indexPath)), ReVerbIndexBuilder.indexWriterConfig(ramBufferMb))
    new ReVerbIndexModifier(indexWriter, Some(localLinker.get), ramBufferMb, linesPerCommit)
  }

  val tabSplitter = "\t".r

  val localLinker = new ThreadLocal[ScoobiEntityLinker]() { override def initialValue = ScoobiEntityLinker.getEntityLinker }
  
  def main(args: Array[String]): Unit = {

    var linesPerCommit = 25000
    var ramBufferMb = 250 
    
    var indexPaths: Seq[String] = Nil
    var inputGroups = false
    var corpus = ""

    val optionParser = new OptionParser() {
      arg("indexPaths", "Colon-delimited list of paths to indexes", { str => indexPaths = str.split(":") })
      arg("corpus", "The corpus identifier to use, e.g. news", { str => corpus = str})
      opt("linesPerCommit", "Lines added across all indexes between commits", { str => linesPerCommit = str.toInt })
    }

    // bail if the args are bad
    if (!optionParser.parse(args)) return

    val parModifier = new ParallelReVerbIndexModifier(indexPaths, ramBufferMb, linesPerCommit)
    
    val lines = Source.fromInputStream(System.in).getLines
    
    val groups = lines flatMap ReVerbExtraction.deserializeFromString map { extr => 
      val key = extr.indexGroupingKey
      val tempInstance = new Instance[ReVerbExtraction](extr, corpus, -1.0)
      val confInstance = ScoobiGroupReGrouper.tryAddConf(tempInstance)
      new ExtractionGroup[ReVerbExtraction](
        new ExtractionArgument(key._1, None, Set.empty),
        new ExtractionRelation(key._2),
        new ExtractionArgument(key._3, None, Set.empty),
        Set(confInstance)
      )
    }
    
    parModifier.updateAll(groups)

    parModifier.close
    
    System.err.println("End of file - normal termination")
  }
}