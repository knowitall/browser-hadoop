package edu.washington.cs.knowitall.browser.lucene

import edu.washington.cs.knowitall.browser.hadoop.scoobi.ScoobiEntityLinker

import edu.washington.cs.knowitall.browser.extraction.ExtractionGroup
import edu.washington.cs.knowitall.browser.extraction.ReVerbExtractionGroup
import edu.washington.cs.knowitall.browser.extraction.ReVerbExtraction

import org.apache.lucene.index.IndexWriter
import org.apache.lucene.store.FSDirectory

import java.io.File

import scopt.OptionParser

import scala.io.Source

class ParallelReVerbIndexModifier(val basicModifiers: Seq[ReVerbIndexModifier], groupsPerCommit: Int) extends IndexModifier {

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
      case Some(e) => throw e // epic hacks... to avoid parallel exception handling
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
}

object ParallelReVerbIndexModifier {
  
  var linesPerCommit = 50000
  var ramBufferMb = 250 
  val tabSplitter = "\t".r

  val localLinker = new ThreadLocal[ScoobiEntityLinker]() { override def initialValue = ScoobiEntityLinker.getEntityLinker }
  
  def main(args: Array[String]): Unit = {

    var indexPaths: Seq[String] = Nil

    val optionParser = new OptionParser() {
      arg("indexPaths", "Colon-delimited list of paths to indexes", { str => indexPaths = str.split(":") })
      opt("linesPerCommit", "Lines added across all indexes between commits", { str => linesPerCommit = str.toInt })
    }

    // bail if the args are bad
    if (!optionParser.parse(args)) return

    val indexWriters = indexPaths.map { indexPath =>
      val indexWriter = new IndexWriter(FSDirectory.open(new File(indexPath)), ReVerbIndexBuilder.indexWriterConfig(ramBufferMb))
      indexWriter
    }

    
    val basicModifiers = indexWriters.map(new ReVerbIndexModifier(_, Some(localLinker.get), ramBufferMb, linesPerCommit))

    val parModifier = new ParallelReVerbIndexModifier(basicModifiers, linesPerCommit)
    
    val lines = Source.fromInputStream(System.in).getLines
    
    val groups = lines flatMap ReVerbExtractionGroup.deserializeFromString
    
    parModifier.updateAll(groups)

    indexWriters foreach(_.close)
    
    System.err.println("End of file - normal termination")
  }
}