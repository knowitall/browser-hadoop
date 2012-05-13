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

import scopt.OptionParser

import java.util.ArrayList

import edu.washington.cs.knowitall.common.Resource.using

class FbTypeLookup(val entityToTypeIntMap: Map[String, Seq[Int]], val typeIntToTypeStringMap: Map[Int, String]) {
  // typeIntToTypeStringMap could probably just be an indexedSeq for a slight performance gain,
  // but then you have to deal with the chance that some int isn't in the enumeration separately.

  def this(entityFile: String, typeEnumFile: String) = this(FbTypeLookup.loadEntityFile(entityFile), FbTypeLookup.loadEnumFile(typeEnumFile))

  /** please strip off the /m/ first. */
  def getTypesForEntity(entityFbid: String): Seq[String] = {

    entityToTypeIntMap.get(entityFbid) match {
      case Some(typeInts) => typeInts.flatMap(typeInt => typeIntToTypeStringMap.get(typeInt))
      case None => Nil
    }
  }
}

/** Convenience struct for helping serialize the lookup table to disk .. */
@SerialVersionUID(1337L)
case class FbPair(val entityName: String, val typeEnumInts: ArrayList[Int])
// this is used as metadata to signify the last object in a serialized file of FbPairs
case object FbPairEOF extends FbPair("This signifies EOF!!!", new ArrayList[Int](0))

object FbTypeLookup {

  import FbTypeLookupGenerator.commaRegex
  import FbTypeLookupGenerator.tabRegex

  def loadEntityFile(entityFile: String): Map[String, Seq[Int]] = {
    var entriesLoaded = 0
    System.err.println("Loading fb entity lookup map...")
    using(new ObjectInputStream(new FileInputStream(entityFile))) { fbPairsInput =>
      
      val fbPairs = Iterator.continually(fbPairsInput.readObject().asInstanceOf[FbPair]).takeWhile(!FbPairEOF.equals(_))
      fbPairs.map(pair=>(pair.entityName, pair.typeEnumInts.toSeq))
    } toMap
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

      arg("entityToTypeNumFile", "output file to contain entity to type enum data", { str => entityToTypeNumFile = str })
      arg("typeEnumFile", "output file to contain type enumeration", { str => typeEnumFile = str })
    }

    if (!parser.parse(args)) return

    val parsedLines = Source.fromInputStream(System.in).getLines.flatMap(parseLine(_))

    val typesToInts = new mutable.HashMap[String, Int]
    var nextTypeInt = 0

    println("Reading file...")

    // convert maps to lists of entry pairs and serialize to disk.
    val entityOutputStream = new ObjectOutputStream(new FileOutputStream(entityToTypeNumFile))
    
    parsedLines.foreach { parsedLine =>

      val typeInts = parsedLine.typeStrings.map { typeString =>
        typesToInts.getOrElseUpdate(typeString, { val next = nextTypeInt; nextTypeInt += 1; next })
      }.sorted
      
      val enumInts = new ArrayList(typeInts)
      val fbPair = FbPair(parsedLine.entityFbid, enumInts)
      
      entityOutputStream.writeObject(fbPair)
    }
    // INSERT END OF FILE IDENTIFIER
    entityOutputStream.writeObject(FbPairEOF)
    
    entityOutputStream.flush()
    entityOutputStream.close()

    val enumWriter = new PrintWriter(typeEnumFile)

    typesToInts.iterator.toSeq.sortBy(_._2).foreach {
      case (typeString, typeInt) =>
        enumWriter.println("%s\t%s".format(typeInt, typeString))
    }

    enumWriter.flush()
    enumWriter.close()

    println("Finished.")
  }
}