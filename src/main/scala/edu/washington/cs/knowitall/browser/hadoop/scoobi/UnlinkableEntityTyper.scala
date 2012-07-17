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

import scala.io.Source

import UnlinkableEntityTyper.REG
// Types are represented as Ints in some places to save space. An file defines an enumeration mapping them back to strings.

sealed abstract class ArgField { 
  def getArgNorm(reg: REG): String 
  def getTypeStrings(reg: REG): Set[String]
  def attachTypes(reg: REG, typeInts: Seq[Int]): REG
  def loadEntityInfo(group: REG): Option[EntityInfo]
  protected def fbTypeToString(fbType: FreeBaseType): String = "/%s/%s".format(fbType.domain, fbType.typ)
  // Silently returns none
  protected def intToFbType(typeInt: Int): Option[FreeBaseType] = {
    val typeInfo = TypeEnumUtils.typeEnumMap.get(typeInt).getOrElse { return None }
    FreeBaseType.parse(typeInfo.typeString)
  }
  // reports return of None to stderr
  protected def intToFbTypeVerbose(typeInt: Int): Option[FreeBaseType] = {
    val fbTypeOpt = intToFbType(typeInt)
    if (!fbTypeOpt.isDefined) System.err.println("Couldn't parse type int: %d".format(typeInt))
    fbTypeOpt
  }
  protected def typeToInt(typ: String): Int = TypeEnumUtils.typeStringMap(typ).enum  
}

case class Arg1() extends ArgField { 
  override def getArgNorm(reg: REG) = reg.arg1.norm 
  override def getTypeStrings(reg: REG) = reg.arg1.types map fbTypeToString filter TypeEnumUtils.typeFilter
  override def attachTypes(reg: REG, typeInts: Seq[Int]) = reg.copy(arg1 = reg.arg1.copy(types = typeInts flatMap intToFbType toSet))
  override def loadEntityInfo(reg: REG): Option[EntityInfo] = reg.arg1.entity map { e =>
    EntityInfo(e.fbid, getTypeStrings(reg) map typeToInt)
  }
}
case class Arg2() extends ArgField {
  override def getArgNorm(reg: REG) = reg.arg2.norm
  override def getTypeStrings(reg: REG) = reg.arg2.types map fbTypeToString filter TypeEnumUtils.typeFilter
  override def attachTypes(reg: REG, typeInts: Seq[Int]) = reg.copy(arg2 = reg.arg2.copy(types = typeInts flatMap intToFbType toSet))
  override def loadEntityInfo(reg: REG): Option[EntityInfo] = reg.arg2.entity map { e =>
    EntityInfo(e.fbid, getTypeStrings(reg) map typeToInt)
  }
}

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

case class RelInfo(val relString: String, val weight: Double, val entities: Set[EntityInfo]) {
  override def toString = "%s:%.02f:%s".format(relString, weight, entities.mkString(":"))
}
case object RelInfo {
  def fromString(str: String) = {
    val split = str.split(":")
    val relString = split(0)
    val weight = split(1).toDouble
    val entities = split.drop(2) map EntityInfo.fromString
    RelInfo(relString, weight, entities.toSet)
  }
}

class UnlinkableEntityTyper(val argField: ArgField) {

  import UnlinkableEntityTyper.{ REG, allPairs, tabSplit }
  import TypeEnumUtils.typeStringMap
  import scala.util.Random
  
  import edu.washington.cs.knowitall.browser.lucene.ExtractionGroupFetcher.entityStoplist

  val debugChecks = true
  
  val maxSimilarEntities = 15
  
  val minTypesShared = 0
  
  val maxPredictedTypes = 20
  
  val minRelWeight = 0.05
  
  val maxEntitiesReadPerRel = 50000
  val maxEntitiesWritePerRel = 500

  def getOptReg(regString: String): Option[REG] = ReVerbExtractionGroup.fromTabDelimited(tabSplit.split(regString))._1

  def getOptRelInfo(relRegs: Iterable[REG]): Option[RelInfo] = {

    val headRelNorm = relRegs.head.rel.norm
    
    if (debugChecks) require {
      relRegs.forall(_.rel.norm.equals(headRelNorm))
    }
   
    def entityBlacklistFilter(entity: EntityInfo): Boolean = !entityStoplist.contains(entity.fbid)
    def typelessEntityFilter(entity: EntityInfo): Boolean = !entity.types.isEmpty
    
    val readEntities = relRegs flatMap argField.loadEntityInfo filter entityBlacklistFilter filter typelessEntityFilter take (maxEntitiesReadPerRel)
    val writeEntities = Random.shuffle(readEntities).take(maxEntitiesWritePerRel)
    
    val relWeight = calculateRelWeight(writeEntities.toIndexedSeq)
    if (relWeight < minRelWeight || writeEntities.isEmpty) None
    else Some(RelInfo(headRelNorm, relWeight, writeEntities.toSet))
  }

  // returns rel string, group string
  def relationRegKV(group: REG): (String, String) = (group.rel.norm, ReVerbExtractionGroup.toTabDelimited(group))
  
    // returns rel string, group string
  def argumentRegKV(group: REG): (String, String) = (argField.getArgNorm(group), ReVerbExtractionGroup.toTabDelimited(group))
  
  // returns arg string, relinfo, group string
  def argRelInfo(relInfo: RelInfo)(group: REG): (String, String) = (argField.getArgNorm(group), relInfo.toString)

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
  
  // Performs the "find similar entities" step described in the paper
  def getTopEntitiesForArg(relInfos: Iterable[RelInfo]): Seq[EntityInfo] = {
    // flatten entities and their weights
    def expWeight(weight: Double) = math.pow(10, 4*weight) // this is what tom found to work as described in the paper.
    
    val entitiesWeighted = relInfos.flatMap { relInfo => 
      relInfo.entities.map(ent => (ent, expWeight(relInfo.weight)))
    }
    // now group by entity and sum the weight
    val topEntities = entitiesWeighted.groupBy(_._1).iterator.map { case (entity, entGroup) => 
      (entity, entGroup.map(_._2).sum)  
    }.toSeq.sortBy(-_._2).take(maxSimilarEntities)
    topEntities.map(_._1)
  }
  
  // returns type enum int, #shared. Seq.empty if no prediction.
  def predictTypes(topEntities: Seq[EntityInfo]): Seq[(Int, Double)] = {
    
    // flatMap the entities to types
    def toTypes(entity: EntityInfo) = entity.types.iterator
    val types = topEntities flatMap toTypes
    // type, #shared
    val typesCounted = types.groupBy(identity).map { case (typeInt, typeGroup) => 
      val typeInfoOption = TypeEnumUtils.typeEnumMap.get(typeInt)
      val shareScore = typeInfoOption match {
        case Some(typeInfo) => {
          val c = typeInfo.instances.toDouble
          val s = maxSimilarEntities.toDouble
          val n = typeGroup.size.toDouble
          math.max(n/s, n/c)
        }
        case None => 1.0 / maxSimilarEntities.toDouble
      }
      (typeInt, shareScore) 
    }
    typesCounted.filter(_._2 >= minTypesShared).toSeq.sortBy(-_._2).take(maxPredictedTypes)
  }
  
  def tryAttachTypes(types: Seq[Int])(reg: REG): REG = {
    if (argField.getTypeStrings(reg).isEmpty) argField.attachTypes(reg, types) else reg
  }
}

object UnlinkableEntityTyper extends ScoobiApp {

  val tabSplit = "\t".r

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
    
    // (REG) elements
    val regs = lines flatMap typer.getOptReg

    // (relation, REG w/ relation) pairs
    // first, we want to group by relation in order to compute relation weight and entity range. 
    val relRegPairs = regs map typer.relationRegKV

    // (relation, Iterable[REG w/ relation]), e.g. the above, grouped by the first element.
    // begin the reduce phase by calling groupByKey 
    val relRegGrouped = relRegPairs.groupByKey

    // (relation, RelInfo) pairs
    val relInfoPairs = relRegGrouped flatMap { case (relString, relRegStrings) => 
      val relRegs = relRegStrings flatMap typer.getOptReg
      typer.getOptRelInfo(relRegs).map(relInfo => (relString, relInfo.toString))
    }

    // (relation, Singleton[RelInfo], Groups of REG w/ relation) 
    // groups of relInfoPairs in the result are always singleton iterables, since there is only one relInfo per rel.
    val relInfoRegGrouped = Relational.coGroup(relInfoPairs, relRegPairs)
    
    // (argument, RelInfo) pairs
    val argRelInfoPairs: DList[(String, String)] = {
      relInfoRegGrouped.flatMap { case (relString, (relInfoSingleton, relRegStrings)) => 
       
      	val relInfoStringOpt = relInfoSingleton.headOption
      	val relRegs = relRegStrings flatMap typer.getOptReg
      	// in-memory group by argument
      	def argStrings: Set[String] = relRegs.map(argField.getArgNorm _).toSet
      	// attach relInfo to every argRelReg 
      	relInfoStringOpt match {
      	  case Some(relInfoString) => {
      	    argStrings.toSeq.map(argString => (argString, relInfoString))
      	  }
      	  case None => Seq.empty
      	}
      }
    }
    
    // (argument, REG w/ argument) pairs
    val argRegPairs = regs map typer.argumentRegKV
    
    // (argument, (Iterable[RelInfos for arg], Iterable[REG w/ arg]))
    val argRelInfosArgRelRegsGrouped: DList[(String, (Iterable[String], Iterable[String]))] = Relational.coGroup(argRelInfoPairs, argRegPairs)

    def getNotableRels(relInfos: Seq[RelInfo]): Seq[RelInfo] = {
      val descending = relInfos.sortBy(-_.weight)
      val best = descending.take(4)
      val worst = descending.takeRight(4)
      val combined = (best ++ worst)
      val deduped = combined.toSet.iterator.toSeq
      val sorted = deduped.sortBy(-_.weight)
      sorted
    }
    
    // (REG)
    val typedRegs = argRelInfosArgRelRegsGrouped flatMap { case (argString, (relInfoStrings, argRelRegStrings)) =>
      val relInfos = relInfoStrings map RelInfo.fromString toSeq // debug: remove toSeq
      val notableRels = getNotableRels(relInfos) map(ri => "%s:%.04f".format(ri.relString, ri.weight)) // debug, deleteme
      val topEntitiesForArg = typer.getTopEntitiesForArg(relInfos)
      val predictedTypes = typer.predictTypes(topEntitiesForArg)
      // now *try* to attach these predicted types to REGs (don't if REG is linked already)
      val argRelRegs = argRelRegStrings flatMap typer.getOptReg
      // try to assign types to every REG in argRelRegs
      //argRelRegs map typer.tryAttachTypes(predictedTypes)
      Seq((argString, predictedTypes, notableRels, topEntitiesForArg.take(5).map(_.fbid)))
    }
    
    // (REG String)
    //val finalResult: DList[String] = typedRegs map ReVerbExtractionGroup.toTabDelimited
    
    // this entire method can be thrown away when done debugging
    val finalResult: DList[String] = typedRegs map { case (argString, predictedTypes, notableRels, topEntitiesForArg) =>
      val types = predictedTypes.flatMap { case (typeInt, numShared) =>
        TypeEnumUtils.typeEnumMap.get(typeInt).map(typeInfo => (typeInfo.typeString, numShared))
      }
      val typesNumShared = types.map({ case (ts, num) => "%s@%.02f".format(ts, num) }).mkString(",")
      val rels = notableRels.mkString(",")
      val entities = topEntitiesForArg.mkString(",")
      Seq(argString, typesNumShared, rels, entities).mkString("\t")
    }
    
    persist(toTextFile(finalResult, outputPath + "/"))
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

  case class TypeInfo(val typeString: String, val enum: Int, val instances: Int)
  
  val typeEnumFile = "/fbTypeEnum.txt"
  val typeBlacklistFile = "/type_blacklist.txt"
  
  def getResourceSource(resourceName: String): Source = Source.fromInputStream(UnlinkableEntityTyper.getClass.getResource(resourceName).openStream)
  def getEnumSource = getResourceSource(typeEnumFile)
  // type, num, count
  def parseEnumLine(line: String): TypeInfo = { val split = tabSplit.split(line); TypeInfo(split(1), split(0).toInt, split(2).toInt) }
  
  lazy val typeStringMap = using(getEnumSource) { _.getLines map parseEnumLine map(ti => (ti.typeString, ti)) toMap }
  lazy val typeEnumMap = using(getEnumSource) { _.getLines map parseEnumLine map(ti => (ti.enum, ti)) toMap }
  lazy val typeBlacklist = using(getResourceSource(typeBlacklistFile)) { _.getLines toSet }
  
  def typeFilter(typeString: String): Boolean = {
    !typeString.startsWith("/base/") && !typeBlacklist.contains(typeString)
  }
}