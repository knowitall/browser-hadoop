package edu.washington.cs.knowitall.browser.hadoop.scoobi

import com.nicta.scoobi.Scoobi._

import com.nicta.scoobi.lib.Relational

import edu.washington.cs.knowitall.common.Timing._
import edu.washington.cs.knowitall.browser.extraction.ReVerbExtraction
import edu.washington.cs.knowitall.browser.extraction.FreeBaseEntity
import edu.washington.cs.knowitall.browser.extraction.FreeBaseType
import edu.washington.cs.knowitall.browser.extraction.Instance
import edu.washington.cs.knowitall.browser.extraction.ExtractionGroup
import edu.washington.cs.knowitall.browser.util.TaggedStemmer
import edu.washington.cs.knowitall.browser.extraction.ReVerbExtractionGroup

import edu.washington.cs.knowitall.tool.chunk.OpenNlpChunker
import edu.washington.cs.knowitall.tool.chunk.ChunkedToken

import scopt.OptionParser

import scala.collection.mutable

case class EntityInfo(val fbid: String, val types: Set[Int]) {
  override def toString = "%s,%s".format(fbid, types.mkString(","))
}
case object EntityInfo {
  def fromString(str: String) = {
    val split = str.split(",")
    val fbid = split(0)
    val types = split.drop(1).map(_.toInt).toSet
    EntityInfo(fbid, types)
  }
}

case class RelInfo(val entities: Set[EntityInfo], val weight: Double) {
  override def toString = "%.02f:%s".format(weight, entities.mkString(":"))
}
case object RelInfo {
  def fromString(str: String) = {
    val split = str.split(":")
    val weight = split(0).toDouble
    val entities = split.drop(1) map EntityInfo.fromString
    RelInfo(entities.toSet, weight)
  }
}

class UnlinkableEntityTyper(val argField: UnlinkableEntityTyper.ArgField) {

  import UnlinkableEntityTyper.{ ArgField, Arg1, Arg2, REG, allPairs, tabSplit }
  import TypeEnumUtils.typeToInt

  val maxEntitiesPerRel = 50000

  def typeToString(typ: FreeBaseType): String = "/%s/%s".format(typ.domain, typ.typ)

  def lineToOptGroup(line: String): Option[REG] = ReVerbExtractionGroup.fromTabDelimited(tabSplit.split(line))._1

  // Converts an REG into entity fbid and types.
  def loadEntityInfo(group: REG): Option[EntityInfo] = argField match {
    case Arg1() => group.arg1.entity match {
      case Some(entity) => Some(EntityInfo(entity.fbid, group.arg1.types map typeToString map typeToInt))
      case None => None
    }
    case Arg2() => group.arg2.entity match {
      case Some(entity) => Some(EntityInfo(entity.fbid, group.arg2.types map typeToString map typeToInt))
      case None => None
    }
  }

  def getRelInfo(relGroupStrings: Iterable[String]): RelInfo = {

    val entities = relGroupStrings.iterator flatMap lineToOptGroup flatMap loadEntityInfo take (maxEntitiesPerRel) toSet

    val relWeight = calculateRelWeight(entities.toIndexedSeq)

    RelInfo(entities, relWeight)
  }

  // returns rel string, group string
  def mapper1KeyValue(group: REG): (String, String) = (group.rel.norm, ReVerbExtractionGroup.toTabDelimited(group))
  
  // returns arg string, relinfo, group string
  def mapper2KeyValue(relInfo: RelInfo)(group: REG): (String, (String, String)) = argField match {
    case Arg1() => (group.arg1.norm, (relInfo.toString, ReVerbExtractionGroup.toTabDelimited(group)))
    case Arg2() => (group.arg2.norm, (relInfo.toString, ReVerbExtractionGroup.toTabDelimited(group)))
  }

  // Input elements are (fbid, count, types)
  def calculateRelWeight(entities: IndexedSeq[EntityInfo]): Double = {

    if (entities.size <= 1) return 0.0
    // now we perform the summation tom describes 
    // the first map produces the terms of the sum
    val terms = allPairs(entities) map {
      case (info1, info2) =>

        val types1 = info1.types
        val types2 = info2.types
        // do types1 and types2 intersect? Computing full intersection is unnecessary.
        if (types1.exists(types2.contains(_))) 1.0 else 0.0
    }
    // we sum the terms and then apply tom's denominator 
    val domainSize = entities.size.toDouble
    val denominator = (domainSize * (domainSize - 1.0)) / 2.0
    terms.sum / denominator
  }
}

object UnlinkableEntityTyper extends ScoobiApp {

  val tabSplit = "\t".r
  
  sealed abstract class ArgField
  case class Arg1() extends ArgField
  case class Arg2() extends ArgField

  type REG = ExtractionGroup[ReVerbExtraction]

  def run() = {

    var inputPath, outputPath = ""
    var argField: ArgField = Arg1()

    val parser = new OptionParser() {
      arg("inputPath", "hdfs input path, ExtractionGroups", { str => inputPath = str })
      arg("outputPath", "hdfs output path, ExtractionGroups", { str => outputPath = str })
      arg("arg", "arg1 to predict types for arg1's, arg2 to predict types for arg2s", { str =>
        if (str.equals("arg1")) argField = Arg1()
        else if (str.equals("arg2")) argField = Arg2()
        else throw new IllegalArgumentException("arg must be either arg1 or arg2")
      })
    }

    if (!parser.parse(args)) System.exit(1)
    
    this.configuration.jobNameIs("Unlinkable-Type-Prediction")
    
    val typer = new UnlinkableEntityTyper(argField)

    // serialized ReVerbExtractions
    val lines: DList[String] = TextInput.fromTextFile(inputPath)
    val groups = lines flatMap typer.lineToOptGroup

    // first, we want to group by relation in order to compute relation weight and entity range. 
    val mapper1Pairs = groups map typer.mapper1KeyValue

    // begin the reduce phase by calling groupByKey 
    val groupedByRelation = mapper1Pairs.groupByKey

    // map groupedByRelation to a list of (rel string, rel entityInfos, rel weight)
    val relInfoPairs = groupedByRelation map { case (relString, groupsWithRel) => (relString, typer.getRelInfo(groupsWithRel).toString) }

    // this is an entire map-reduce job, as far as I can tell:
    val relInfosWithGroups = Relational.coGroup(relInfoPairs, mapper1Pairs)
    
    // now we want to re-group by argField, keeping relInfoPairs with each new group.
    // DList[Arg String, (RelInfoString, RelGroupStrings)]
    val mapper3Pairs = relInfosWithGroups.flatMap { case (relString, (relInfoString, relGroups)) =>
      val relInfo = RelInfo.fromString(relInfoString.head)
      relGroups flatMap typer.lineToOptGroup map typer.mapper2KeyValue(relInfo)
    }
    
    val groupedByArg = mapper3Pairs.groupByKey
    
    persist(toTextFile(groupedByArg, outputPath + "/"))
  }

  // this belongs in a util package somewhere
  private def allPairs[T](input: IndexedSeq[T]): Iterator[(T, T)] = {

    val length = input.length

    (0 until length).iterator.flatMap { i =>
      (i + 1 until length).map { j =>
        (input(i), input(j))
      }
    }
  }
}

object TypeEnumUtils {
  
  import edu.washington.cs.knowitall.common.Resource.using
  
  import UnlinkableEntityTyper.tabSplit
  
  val typeEnumFile = "/fbTypeEnum.txt"
  def getEnumSource = io.Source.fromInputStream(UnlinkableEntityTyper.getClass.getResource(typeEnumFile).openStream)
  def enumLineToTypeInt(line: String): (String, Int) = { val split = tabSplit.split(line); (split(1), split(0).toInt) }
  
  private lazy val typeToIntMap = using(getEnumSource) { _.getLines map enumLineToTypeInt toMap }
  private lazy val intToTypeMap = using(getEnumSource) { _.getLines map enumLineToTypeInt map(_.swap) toMap }
  
  def typeToInt(typ: String): Int = typeToIntMap(typ)
  def intToType(int: Int): String = intToTypeMap(int)
}