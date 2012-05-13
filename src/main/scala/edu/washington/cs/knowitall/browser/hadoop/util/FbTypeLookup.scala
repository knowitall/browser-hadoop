package edu.washington.cs.knowitall.browser.hadoop.util

import scala.collection.mutable
import scala.io.Source

import scala.collection.JavaConversions._
import scala.collection.immutable.SortedMap
import scala.collection.immutable.TreeMap

import java.io.File
import java.io.FileOutputStream
import java.io.ObjectOutputStream
import java.io.FileInputStream
import java.io.ObjectInputStream
import java.io.PrintWriter

import org.apache.lucene.document.Document
import org.apache.lucene.document.Field
import org.apache.lucene.document.Field.Store
import org.apache.lucene.document.Field.Index
import org.apache.lucene.index.IndexWriter
import org.apache.lucene.util.Version
import org.apache.lucene.index.IndexWriterConfig
import org.apache.lucene.index.Term
import org.apache.lucene.index.IndexReader
import org.apache.lucene.analysis.WhitespaceAnalyzer
import org.apache.lucene.store.FSDirectory
import org.apache.lucene.search.IndexSearcher
import org.apache.lucene.search.TermQuery

import org.apache.lucene.queryParser.QueryParser

import scopt.OptionParser

import java.util.ArrayList
import java.util.LinkedList
import java.io.File

import edu.washington.cs.knowitall.common.Resource.using

class FbTypeLookup(val searcher: IndexSearcher, val typeIntToTypeStringMap: Map[Int, String]) {
  // typeIntToTypeStringMap could probably just be an indexedSeq for a slight performance gain,
  // but then you have to deal with the chance that some int isn't in the enumeration separately.
  // delete this if this class actually works: private val queryParser = new QueryParser(Version.LUCENE_36, "fbid", new WhitespaceAnalyzer(Version.LUCENE_36))
  
  def this(indexPath: String, typeEnumFile: String) = this(FbTypeLookup.loadIndex(indexPath), FbTypeLookup.loadEnumFile(typeEnumFile))

  /** please strip off the /m/ first. */
  def getTypesForEntity(entityFbid: String): Seq[String] = {
     val query = new TermQuery(new Term("fbid", entityFbid))
     val hits = searcher.search(query, null, 10)
     hits.scoreDocs.map(_.doc).map(searcher.doc(_)).flatMap { doc =>
       val fbid = doc.get("fbid")
       require(fbid.equals(entityFbid))
       val typeEnumInts = doc.get("types").split(",").map(_.toInt)
       typeEnumInts.flatMap(typeIntToTypeStringMap.get(_))
     }
  }
}

/** Convenience struct for helping serialize the lookup table to disk .. */
@SerialVersionUID(1337L)
case class FbPair(val entityFbid: String, val typeEnumInts: ArrayList[Int]) {
  def toDocument: Document = {
    val doc = new Document()
    doc.add(new Field("fbid", entityFbid, Store.YES, Index.NOT_ANALYZED))
    doc.add(new Field("types", typeEnumInts.mkString(","), Store.YES, Index.NO))
    doc
  }
}

object FbTypeLookup {

  import FbTypeLookupGenerator.commaRegex
  import FbTypeLookupGenerator.tabRegex
  
  def loadIndex(path: String): IndexSearcher = {
    val dir = FSDirectory.open(new File(path))
    val indexReader = IndexReader.open(dir, true)
    new IndexSearcher(indexReader)
  }
  
  def loadEnumFile(enumFile: String): SortedMap[Int, String] = {
    System.err.println("Loading type enumeration...")
    using(Source.fromFile(enumFile)) { source =>
      val elements = source.getLines.flatMap { line =>
        tabRegex.split(line) match {
          case Array(typeInt, typeString) => Some((typeInt.toInt, typeString))
          case _ => { System.err.println("Bad enum line:%s".format(line)); None }
        }
      }
      TreeMap.empty[Int, String] ++ elements.toMap
    }
  }

  def main(args: Array[String]): Unit = {
    var entityFile = ""
    var enumFile = ""
    val parser = new OptionParser() {

      arg("entityToTypeNumFile", "output file to contain entity to type enum data", { str => entityFile = str })
      arg("typeEnumFile", "output file to contain type enumeration", { str => enumFile = str })
    }
    if (!parser.parse(args)) return
    
    val lookup = new FbTypeLookup(entityFile, enumFile)

    val fbids = Seq("03gss12", "0260w54", "0260xrp", "02610rn", "02610t0")
    
    fbids.foreach(line => println("%s, %s".format(line, lookup.getTypesForEntity(line))))
  }
}

/**
  * Generates data for a type lookup table (freebase entity => freebase types)
  */
object FbTypeLookupGenerator {

  val tabRegex = "\t".r
  val commaRegex = ",".r
  val fbidPrefixRegex = "/m/".r

  case class ParsedLine(entityFbid: String, typeStrings: Seq[String])

  def parseLine(line: String): Option[ParsedLine] = {

    def lineFailure = { System.err.println("bad line: %s".format(line)); None }

    tabRegex.split(line) match {
      case Array(rawEntity, rawTypes, _*) => parseSplitLine(rawEntity, rawTypes)
      case Array(rawEntity) => None // some entities don't seem to have any type info associated
      case _ => lineFailure
    }
  }

  def parseSplitLine(rawEntity: String, rawTypes: String): Option[ParsedLine] = {

    // try to remove the /m/ prefix from the entity
    val trimmedEntity = fbidPrefixRegex.findFirstIn(rawEntity) match {
      case Some(string) => rawEntity.substring(3)
      case None => { System.err.println("bad entity string: %s".format(rawEntity)); return None }
    }

    // split the rawTypes by commas
    val splitTypes = commaRegex.split(rawTypes)

    Some(ParsedLine(trimmedEntity, splitTypes))
  }

  def main(args: Array[String]): Unit = {

    var entityToTypeNumFile = ""
    var typeEnumFile = ""

    val parser = new OptionParser() {

      arg("entityToTypeNumFile", "output path for lucene index", { str => entityToTypeNumFile = str })
      arg("typeEnumFile", "output file to contain type enumeration", { str => typeEnumFile = str })
    }

    if (!parser.parse(args)) return

    val parsedLines = Source.fromInputStream(System.in).getLines.flatMap(parseLine(_))

    val typesToInts = new mutable.HashMap[String, Int]
    var nextTypeInt = 0
    var linesDone = 0
    println("Reading file...")

    // convert maps to lists of entry pairs and serialize to disk.
    
    val indexWriter = getIndexWriter(entityToTypeNumFile)
    parsedLines.foreach { parsedLine =>

      val typeInts = parsedLine.typeStrings.map { typeString =>
        typesToInts.getOrElseUpdate(typeString, { val next = nextTypeInt; nextTypeInt += 1; next })
      }.sorted
      
      val enumInts = new ArrayList(typeInts)
      val fbPair = FbPair(parsedLine.entityFbid, enumInts)
      
      indexWriter.addDocument(fbPair.toDocument)
      linesDone += 1;
      if (linesDone % 100000 == 0) System.err.println("Lines done: %s".format(linesDone))
    }
    
    indexWriter.close()

    val enumWriter = new PrintWriter(typeEnumFile)

    typesToInts.iterator.toSeq.sortBy(_._2).foreach {
      case (typeString, typeInt) =>
        enumWriter.println("%s\t%s".format(typeInt, typeString))
    }

    enumWriter.flush()
    enumWriter.close()

    println("Finished.")
  }
  
  private def getIndexWriter(path: String): IndexWriter = {
    val analyzer = new WhitespaceAnalyzer(Version.LUCENE_36)
    val config = new IndexWriterConfig(Version.LUCENE_36, analyzer)
    val dir = FSDirectory.open(new File(path))
    val writer = new IndexWriter(dir, config)
    writer.setInfoStream(System.err)
    writer
  }
}