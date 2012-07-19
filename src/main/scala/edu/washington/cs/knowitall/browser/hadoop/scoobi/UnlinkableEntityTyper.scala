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

case class RelInfo(val weight: Double, val entities: Set[EntityInfo]) {
  override def toString = "%.02f:%s".format(weight, entities.mkString(":"))
}
case object RelInfo {
  def fromString(str: String) = {
    val split = str.split(":")
    val weight = split(0).toDouble
    val entities = split.drop(1) map EntityInfo.fromString
    RelInfo(weight, entities.toSet)
  }
}

class UnlinkableEntityTyper(
    val argField: ArgField, 
    val maxSimilarEntities: Int, 
    val maxPredictedTypes: Int,
    val minShareScore: Int,
    val minRelWeight: Double, 
    val maxEntitiesReadPerRel: Int, 
    val maxEntitiesWritePerRel: Int,
    val maxRelInfosReadPerArg: Int) {

  import UnlinkableEntityTyper.{ REG, allPairs, tabSplit }
  import TypeEnumUtils.typeStringMap
  import scala.util.Random
  
  import edu.washington.cs.knowitall.browser.lucene.ExtractionGroupFetcher.entityStoplist

  def getOptReg(regString: String) = time(getOptRegUntimed(regString), Timers.incParseRegCount _)
  def getOptRegUntimed(regString: String): Option[REG] = ReVerbExtractionGroup.fromTabDelimited(tabSplit.split(regString))._1

  var numRelInfosOutput = 0
  var numRelInfosSkipped = 0
  var numSkippedDueToEmpty = 0
  
  def getOptRelInfo(relEntities: Iterator[EntityInfo]) = time(getOptRelInfoUntimed(relEntities), Timers.incLoadRelInfoCount _)
  def getOptRelInfoUntimed(relEntities: Iterator[EntityInfo]): Option[RelInfo] = {
   
    if (Timers.loadRelInfoCount.count % 500 == 0) System.err.println("num relinfos output: %s, num not output: %s, num empty: %s".format(numRelInfosOutput, numRelInfosSkipped, numSkippedDueToEmpty))
   
    val readEntities = relEntities take(maxEntitiesReadPerRel)
    val writeEntities = Random.shuffle(readEntities.toSeq).take(maxEntitiesWritePerRel)
    
    val relWeight = calculateRelWeight(writeEntities.toIndexedSeq)
    if (relWeight < minRelWeight || writeEntities.isEmpty) {
      numRelInfosSkipped += 1
      if (writeEntities.isEmpty) numSkippedDueToEmpty += 1
      None
    }
    else {
      numRelInfosOutput += 1
      Some(RelInfo(relWeight, writeEntities.toSet))
    }
  }

  // returns rel string, group string
  def relationEntityKv(group: REG) = time(relationRegKvUntimed(group), Timers.incRelRegCount _)
  def relationRegKvUntimed(group: REG): Option[(String, String)] = {
    def entityBlacklistFilter(entity: EntityInfo): Boolean = !entityStoplist.contains(entity.fbid)
    def typelessEntityFilter(entity: EntityInfo): Boolean = !entity.types.isEmpty
    argField.loadEntityInfo(group) filter entityBlacklistFilter filter typelessEntityFilter map { entityInfo => (group.rel.norm, entityInfo.toString) }
  }

  def relationArgKv(group: REG): (String, String) = (group.rel.norm, argField.getArgNorm(group))
  
    // returns rel string, group string
  def argumentRegKv(group: REG): (String, String) = time(argumentRegKvUntimed(group), Timers.incArgRegCount _) 
  def argumentRegKvUntimed(group: REG): (String, String) = (argField.getArgNorm(group), ReVerbExtractionGroup.toTabDelimited(group))
  
  // returns arg string, relinfo, group string
  def argRelInfo(relInfo: RelInfo)(group: REG): (String, String) = time(argRelInfoUntimed(relInfo)(group), Timers.incArgRelInfoCount _) 
  def argRelInfoUntimed(relInfo: RelInfo)(group: REG): (String, String) = (argField.getArgNorm(group), relInfo.toString)

  // Input elements are (fbid, count, types)
  def calculateRelWeight(entities: IndexedSeq[EntityInfo]) = time(calculateRelWeightUntimed(entities), Timers.incRelWeightCount _)
  def calculateRelWeightUntimed(entities: IndexedSeq[EntityInfo]): Double = {

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
  def getTopEntitiesForArg(relInfos: Iterable[RelInfo]) = time(getTopEntitiesForArgUntimed(relInfos), Timers.incGetTopEntitiesCount _)
  def getTopEntitiesForArgUntimed(relInfos: Iterable[RelInfo]): Seq[EntityInfo] = {
    // flatten entities and their weights
    def expWeight(weight: Double) = math.pow(10, 4*weight) // this is what tom found to work as described in the paper.
    
    val entitiesWeighted = relInfos.take(maxRelInfosReadPerArg).flatMap { relInfo => 
      relInfo.entities.map(ent => (ent, expWeight(relInfo.weight)))
    }
    // now group by entity and sum the weight
    val topEntities = entitiesWeighted.groupBy(_._1).iterator.map { case (entity, entGroup) => 
      (entity, entGroup.map(_._2).sum)  
    }.toSeq.sortBy(-_._2).take(maxSimilarEntities)
    topEntities.map(_._1)
  }
  
  // returns type enum int, #shared. Seq.empty if no prediction.
  def predictTypes(topEntities: Seq[EntityInfo]) = time(predictTypesUntimed(topEntities), Timers.incPredictTypesCount _)
  def predictTypesUntimed(topEntities: Seq[EntityInfo]): Seq[(Int, Int)] = {
    
    // flatMap the entities to types
    def toTypes(entity: EntityInfo) = entity.types.iterator
    val types = topEntities flatMap toTypes
    // type, #shared
    val typesCounted = types.groupBy(identity).map { case (typeInt, typeGroup) => 
      val typeInfoOption = TypeEnumUtils.typeEnumMap.get(typeInt)
//      val shareScore = typeInfoOption match {
//        case Some(typeInfo) => {
//          val c = math.max(typeInfo.instances.toDouble, 1)
//          val s = maxSimilarEntities.toDouble
//          val n = typeGroup.size.toDouble
//          math.max(n/s, n/c)
//        }
//        case None => 1.0 / maxSimilarEntities.toDouble
//      }
      val shareScore = typeGroup.size
      (typeInt, shareScore) 
    }
    typesCounted.toSeq.sortBy(-_._2).take(maxPredictedTypes)
  }
  
  def tryAttachTypes(types: Seq[Int])(reg: REG): REG = {
    if (argField.getTypeStrings(reg).isEmpty) argField.attachTypes(reg, types) else reg
  }
  
  object Timers {
    
    var argRelInfoCount = MutInt.zero
    var argRelInfoTime = MutInt.zero
    
    def incArgRelInfoCount(time: Long): Unit = {
      argRelInfoCount.inc
      argRelInfoTime.add(time)
      bleat(argRelInfoCount, argRelInfoTime, "arg/relinfo pairs: %s, in %s, (Avg: %s)", 2000)
    }
    
    var argRegCount = MutInt.zero
    var argRegTime = MutInt.zero
    
    def incArgRegCount(time: Long): Unit = {
      argRegCount.inc
      argRegTime.add(time)
      bleat(argRegCount, argRegTime, "arg/reg pairs: %s, in %s, (Avg: %s)", 2000)
    }
    
    var relWeightCount = MutInt.zero
    var relWeightTime = MutInt.zero
    
    def incRelWeightCount(time: Long): Unit = {
      relWeightCount.inc
      relWeightTime.add(time)
      bleat(relWeightCount, relWeightTime, "relWeights calculated: %s, in %s, (Avg: %s)", 1000)
    }
    
    var parseRegCount = MutInt.zero
    var parseRegTime = MutInt.zero
    
    def incParseRegCount(time: Long): Unit = {
      parseRegCount.inc
      parseRegTime.add(time)
      bleat(parseRegCount, parseRegTime, "REGs parsed: %s, in %s, (Avg: %s)", 4000)
    }
    
    var loadRelInfoCount = MutInt.zero
    var loadRelInfoTime = MutInt.zero
    
    def incLoadRelInfoCount(time: Long): Unit = {
      loadRelInfoCount.inc
      loadRelInfoTime.add(time)
      bleat(loadRelInfoCount, loadRelInfoTime, "relInfos loaded: %s, in %s, (Avg: %s)", 10000)
    }
    
    var getTopEntitiesCount = MutInt.zero
    var getTopEntitiesTime = MutInt.zero
    
    def incGetTopEntitiesCount(time: Long): Unit = {
      getTopEntitiesCount.inc
      getTopEntitiesTime.add(time)
      bleat(getTopEntitiesCount, getTopEntitiesTime, "calls to getTopEntities: %s, in %s, (Avg: %s)", 1000)
    }
    
    var predictTypesCount = MutInt.zero
    var predictTypesTime = MutInt.zero

    def incPredictTypesCount(time: Long): Unit = {
      predictTypesCount.inc
      predictTypesTime.add(time)
      bleat(predictTypesCount, predictTypesTime, "calls to predictTypes: %s, in %s, (Avg: %s)", 1000)
    }
    
    var relRegCount = MutInt.zero
    var relRegTime = MutInt.zero
    
    def incRelRegCount(time: Long): Unit = {
      relRegCount.inc
      predictTypesTime.add(time)
      bleat(relRegCount, predictTypesTime, "relReg pairs: %s, in %s, (Avg: %s)", 10000)
    }
    
    private def bleat(count: MutInt, time: MutInt, fmtString: String, interval: Int) = {
      val avgTime = time.count / count.count
      if (count.count % interval == 0) System.err.println(fmtString.format(count.count.toString, Seconds.format(time.count), Seconds.format(avgTime)))
    }
  }
}

object UnlinkableEntityTyper extends ScoobiApp {

  val tabSplit = "\t".r

  type REG = ExtractionGroup[ReVerbExtraction]

  def run() = {

    var inputPath, outputPath = ""
    var argField: ArgField = Arg1()

    var maxSimilarEntities = 15
    var maxPredictedTypes = 5
    var minShareScore = 6
    var minRelWeight = 0.10
    var maxEntitiesReadPerRel = 5000
    var maxEntitiesWritePerRel = 150
    var maxRelInfosReadPerArg = 25000

    val parser = new OptionParser() {
      arg("inputPath", "hdfs input path, ExtractionGroups", { str => inputPath = str })
      arg("outputPath", "hdfs output path, ExtractionGroups", { str => outputPath = str })
      arg("arg", "arg1 to predict types for arg1's, arg2 to predict types for arg2s", { str =>
        if (str.equals("arg1")) argField = Arg1()
        else if (str.equals("arg2")) argField = Arg2()
        else throw new IllegalArgumentException("arg must be either arg1 or arg2")
      })
      opt("maxSimilarEntities", "maximum similar entities considered per argument", { str => maxSimilarEntities = str.toInt })
      opt("maxPredictedTypes", "maximum predicated types in final output", { str => maxPredictedTypes = str.toInt })
      opt("minShareScore", "minimum entities sharing a type needed", { str => minShareScore = str.toInt })
      opt("minRelWeight", "minimum rel weight needed to consider entities from rel", { str => minRelWeight = str.toDouble })
      opt("maxEntitiesReadPerRel", "maximum entities read per rel", { str => maxEntitiesReadPerRel = str.toInt })
      opt("maxEntitiesWritePerRel", "maximum entities to write as intermediate output per rel", { str => maxEntitiesWritePerRel = str.toInt })
      opt("maxRelInfosReadPerArg", "maximumRelInfos read into memory per argument", { str => maxRelInfosReadPerArg })
    }

    if (!parser.parse(args)) System.exit(1)
    
    this.configuration.jobNameIs("Unlinkable-Type-Prediction")
    
    val typer = new UnlinkableEntityTyper(
        argField=argField,
        maxSimilarEntities=maxSimilarEntities,
        maxPredictedTypes=maxPredictedTypes,
        minShareScore = minShareScore,
        minRelWeight=minRelWeight,
        maxEntitiesReadPerRel=maxEntitiesReadPerRel,
        maxEntitiesWritePerRel=maxEntitiesWritePerRel,
        maxRelInfosReadPerArg=maxRelInfosReadPerArg
      )

    // serialized ReVerbExtractions
    val lines: DList[String] = TextInput.fromTextFile(inputPath)
    
    // (REG) elements
    val regs = lines flatMap typer.getOptReg

    // (relation, REG w/ relation) pairs
    // first, we want to group by relation in order to compute relation weight and entity range. 
    val relEntityPairs = regs flatMap typer.relationEntityKv

    // (relation, Iterable[REG w/ relation]), e.g. the above, grouped by the first element.
    // begin the reduce phase by calling groupByKey 
    def relEntityGrouped = relEntityPairs.groupByKey

    // (relation, RelInfo) pairs
    val relInfoPairs = relEntityGrouped flatMap { case (relString, relEntityStrings) => 
      val relEntities = relEntityStrings.iterator map EntityInfo.fromString
      typer.getOptRelInfo(relEntities).map(relInfo => (relString, relInfo.toString))
    }

    
    val relArgPairs = regs map typer.relationArgKv
    
    // (relation, Singleton[RelInfo], Groups of REG w/ relation) 
    // groups of relInfoPairs in the result are always singleton iterables, since there is only one relInfo per rel.
    val relInfoRegGrouped = Relational.coGroup(relInfoPairs, relArgPairs)
    
    // (argument, RelInfo, arg string) pairs
    val argRelInfoPairs: DList[(String, String)] = {
      var numRelInfoPairs = 0
      relInfoRegGrouped.flatMap { case (relString, (relInfoSingleton, relArgStrings)) => 
       
      	val relInfoStringOpt = relInfoSingleton.headOption
      	// attach relInfo to every argRelReg 
      	relInfoStringOpt match {
      	  case Some(relInfoString) => {
      	    relArgStrings.map { argString => 
      	      numRelInfoPairs += 1
      	      if (numRelInfoPairs == 1 || numRelInfoPairs % 2000 == 0) System.err.println("num rel info pairs: %s".format(numRelInfoPairs))
      	      (argString, relInfoString) 
      	    }
      	  }
      	  case None => Iterable.empty
      	}
      }
    }
    
    // (argument, REG w/ argument) pairs
    val argRegPairs = regs map typer.argumentRegKv
    
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
      val relInfos = relInfoStrings map RelInfo.fromString // debug: remove toSeq
      //val notableRels = getNotableRels(relInfos) map(ri => "%s:%.04f".format(ri.relString, ri.weight)) // debug, deleteme
      val topEntitiesForArg = typer.getTopEntitiesForArg(relInfos)
      val predictedTypes = typer.predictTypes(topEntitiesForArg)
      // now *try* to attach these predicted types to REGs (don't if REG is linked already)
      //val argRelRegs = argRelRegStrings flatMap typer.getOptReg
      // try to assign types to every REG in argRelRegs
      //argRelRegs map typer.tryAttachTypes(predictedTypes)
      Seq((argString, predictedTypes, topEntitiesForArg.take(5).map(_.fbid)))
    }
    
    // (REG String)
    //val finalResult: DList[String] = typedRegs map ReVerbExtractionGroup.toTabDelimited
    
    // this entire method can be thrown away when done debugging
    val finalResult: DList[String] = typedRegs map { case (argString, predictedTypes, topEntitiesForArg) =>
      val types = predictedTypes.flatMap { case (typeInt, numShared) =>
        TypeEnumUtils.typeEnumMap.get(typeInt).map(typeInfo => (typeInfo.typeString, numShared))
      }
      val typesNumShared = types.map({ case (ts, num) => "%s@%d".format(ts, num) }).mkString(",")
      val entities = topEntitiesForArg.mkString(",")
      Seq(argString, typesNumShared, entities).mkString("\t")
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

case class MutInt(var count: Long) { 
  def inc: Unit = { count += 1 } 
  def add(t: Long) = { count += t }
}
case object MutInt { def zero = MutInt(0) }

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