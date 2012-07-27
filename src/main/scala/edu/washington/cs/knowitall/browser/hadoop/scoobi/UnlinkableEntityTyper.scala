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
import edu.washington.cs.knowitall.browser.hadoop.scoobi.util.EntityInfo
import edu.washington.cs.knowitall.browser.hadoop.scoobi.util.RelInfo
import edu.washington.cs.knowitall.browser.hadoop.scoobi.util.TypeInfo
import edu.washington.cs.knowitall.browser.hadoop.scoobi.util.TypeInfoUtils
import edu.washington.cs.knowitall.browser.hadoop.scoobi.util.TypePrediction
import edu.washington.cs.knowitall.browser.hadoop.scoobi.util.{Arg1, Arg2, ArgField}
import scopt.OptionParser
import scala.collection.mutable
import scala.io.Source
import UnlinkableEntityTyper.REG

class UnlinkableEntityTyper(
    val argField: ArgField, 
    val maxSimilarEntities: Int, 
    val maxPredictedTypes: Int,
    val minShareScore: Int,
    val minRelWeight: Double, 
    val maxEntitiesReadPerRel: Int, 
    val maxEntitiesWritePerRel: Int,
    val maxRelInfosReadPerArg: Int) {

  import UnlinkableEntityTyper.{ REG, allPairs, tabSplit, minArgLength }
  import TypeInfoUtils.typeStringMap
  import scala.util.Random
  import edu.washington.cs.knowitall.browser.lucene.ExtractionGroupFetcher.entityStoplist

  def getOptReg(regString: String) = time(getOptRegUntimed(regString), Timers.incParseRegCount _)
  def getOptRegUntimed(regString: String): Option[REG] = ReVerbExtractionGroup.deserializeFromString(regString)

  var numRelInfosOutput = 0
  var numRelInfosSkipped = 0
  var numSkippedDueToEmpty = 0
  
  private val numPattern = "[0-9][0-9][0-9]+".r
  private val argStopList = Set("one", "two", "three", "four", "five", "some", "any", "all")
  private def filterArgString(str: String) = (str.length >= minArgLength) && (numPattern.findFirstIn(str) match {
    case Some(num) => false
    case None => !tabSplit.split(str).exists(tok => argStopList.contains(tok))
  })
  
  def getOptRelInfo(relString: String, relEntities: Iterator[EntityInfo]) = time(getOptRelInfoUntimed(relString, relEntities), Timers.incLoadRelInfoCount _)
  def getOptRelInfoUntimed(relString: String, relEntities: Iterator[EntityInfo]): Option[RelInfo] = {
   
    if (Timers.loadRelInfoCount.count % 500 == 0) System.err.println("num relinfos output: %s, num not output: %s, num empty: %s".format(numRelInfosOutput, numRelInfosSkipped, numSkippedDueToEmpty))
   
    val readEntities = relEntities take(maxEntitiesReadPerRel)
    val writeEntities = Random.shuffle(readEntities.toSeq).take(maxEntitiesWritePerRel)
    
    lazy val relWeight = calculateRelWeight(writeEntities.toIndexedSeq)
    if (relString.length <= 3 || relString.length > 100 || writeEntities.isEmpty || relWeight < minRelWeight) {
      numRelInfosSkipped += 1
      if (writeEntities.isEmpty) numSkippedDueToEmpty += 1
      None
    }
    else {
      numRelInfosOutput += 1
      Some(RelInfo(relString, relWeight, writeEntities.toSet))
    }
  }

  // returns rel string, group string
  def relationEntityKv(group: REG) = time(relationRegKvUntimed(group), Timers.incRelRegCount _)
  def relationRegKvUntimed(group: REG): Option[(String, String)] = {
    def entityBlacklistFilter(entity: EntityInfo): Boolean = !entityStoplist.contains(entity.fbid)
    def typelessEntityFilter(entity: EntityInfo): Boolean = !entity.types.isEmpty
    argField.loadEntityInfo(group) filter entityBlacklistFilter filter typelessEntityFilter map { entityInfo => (group.rel.norm, entityInfo.toString) }
  }

  def relationArgKv(group: REG): Option[(String, String)] = {
    val argString = argField.getArgNorm(group)
    if (filterArgString(argString)) Some((group.rel.norm, argString))
    else None
  }
  
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
  // returns totalentity weight as a second argument
  def getTopEntitiesForArg(relInfos: Seq[RelInfo]) = time(getTopEntitiesForArgUntimed(relInfos), Timers.incGetTopEntitiesCount _)
  def getTopEntitiesForArgUntimed(relInfos: Seq[RelInfo]): (Seq[EntityInfo], Double) = {
    // flatten entities and their weights
    def expWeight(weight: Double) = math.pow(10, 4*weight) // this is what tom found to work as described in the paper.
    
    val entitiesWeighted = relInfos.flatMap { relInfo => 
      relInfo.entities.map(ent => (ent, expWeight(relInfo.weight)))
    }
    val totalWeight = entitiesWeighted.map(_._2).sum
    // now group by entity and sum the weight
    val topEntities = entitiesWeighted.groupBy(_._1).iterator.map { case (entity, entGroup) => 
      (entity, entGroup.map(_._2).sum)  
    }.toSeq.sortBy(-_._2).take(maxSimilarEntities)
    (topEntities.map(_._1), totalWeight)
  }
  
  // returns type enum int, #shared. Seq.empty if no prediction.
  def predictTypes(topEntities: Seq[EntityInfo]) = time(predictTypesUntimed(topEntities), Timers.incPredictTypesCount _)
  def predictTypesUntimed(topEntities: Seq[EntityInfo]): Seq[(TypeInfo, Int)] = {
    
    // flatMap the entities to types
    def toTypes(entity: EntityInfo) = entity.types.iterator
    val types = topEntities flatMap toTypes
    // type, #shared
    val typesCounted = types.groupBy(identity).flatMap { case (typeInt, typeGroup) => 
      val shareScore = typeGroup.size
      TypeInfoUtils.typeEnumMap.get(typeInt).map { typeInfo => (typeInfo, shareScore) }
    }
    typesCounted.toSeq.filter(_._2 >= minShareScore).sortBy(-_._2).take(maxPredictedTypes)
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
  
  val minArgLength = 4

  type REG = ExtractionGroup[ReVerbExtraction]

  def run() = {

    var inputPath, outputPath = ""
    var argField: ArgField = Arg1()

    var maxSimilarEntities = 15
    var maxPredictedTypes = 5
    var minShareScore = 10
    var minRelWeight = 0.10
    var maxEntitiesReadPerRel = 5000
    var maxEntitiesWritePerRel = 150
    var maxRelInfosReadPerArg = 20000
    var maxRelInfosWritePerArg = 1000
    var keepRelString = false
    var onlyPhaseOne = false
    var onlyPhaseTwo = false

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
      opt("maxRelInfosWritePerArg", "maximumRelInfos written per argument", { str => maxRelInfosReadPerArg })
      opt("keepRelString", "keep relation string in final output for debugging", { keepRelString = true })
      opt("phaseOne", "only run first phase, (arg/relInfo KV pairs)", { onlyPhaseOne = true })
      opt("phaseTwo", "only run second phase, (specify output of phaseOne as inputPath, outputs type predictions)", { onlyPhaseTwo = true })
    }

    if (!parser.parse(args)) System.exit(1)
    
    require(!onlyPhaseOne || !onlyPhaseTwo)
    
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
    
    def phaseOne(rawRegStrings: DList[String]): DList[(String, String)] = {

      // (REG) elements
      val regs = rawRegStrings flatMap typer.getOptReg

      // (relation, REG w/ relation) pairs
      // first, we want to group by relation in order to compute relation weight and entity range. 
      val relEntityPairs = regs flatMap typer.relationEntityKv

      // (relation, Iterable[REG w/ relation]), e.g. the above, grouped by the first element.
      // begin the reduce phase by calling groupByKey 
      def relEntityGrouped = relEntityPairs.groupByKey

      // (relation, RelInfo) pairs
      val relInfoPairs = relEntityGrouped flatMap {
        case (relString, relEntityStrings) =>
          val relEntities = relEntityStrings.iterator map EntityInfo.fromString
          val relStringSwitch = if (keepRelString) relString else "-"
          typer.getOptRelInfo(relString, relEntities).map(relInfo => (relString, relInfo.toString))
      }

      val relArgPairs = regs flatMap typer.relationArgKv

      // (relation, Singleton[RelInfo], Groups of REG w/ relation) 
      // groups of relInfoPairs in the result are always singleton iterables, since there is only one relInfo per rel.
      val relInfoRelArgsGrouped = Relational.coGroup(relInfoPairs, relArgPairs)

      // (argument, RelInfo, arg string) pairs
      val argRelInfoPairs: DList[(String, String)] = {
        var numRelInfoPairs = 0
        relInfoRelArgsGrouped.flatMap {
          case (relString, (relInfoSingleton, relArgStrings)) =>
            val relInfoStringOpt = relInfoSingleton.headOption
            // attach relInfo to every argRelReg 
            relInfoStringOpt match {
              case Some(relInfoString) => {
                relArgStrings.take(maxRelInfosWritePerArg).toSeq.distinct.map { argString =>
                  numRelInfoPairs += 1
                  if (numRelInfoPairs == 1 || numRelInfoPairs % 2000 == 0) System.err.println("num rel info pairs: %s".format(numRelInfoPairs))
                  (argString, relInfoString)
                }
              }
              case None => Iterable.empty
            }
        }
      }
      argRelInfoPairs
    }
    
    def phaseTwo(argRelInfoPairs: DList[(String, String)]): DList[String] = {

      val argRelInfosGrouped: DList[(String, Iterable[String])] = argRelInfoPairs.groupByKey

      def getNotableRels(relInfos: Seq[RelInfo]): Seq[RelInfo] = {
        val descending = relInfos.sortBy(-_.weight)
        val best = descending.take(4)
        val deduped = best.toSet.iterator.toSeq
        val sorted = deduped.sortBy(-_.weight)
        sorted
      }

      // (REG)
      val typePredictionStrings = argRelInfosGrouped flatMap {
        case (argString, relInfoStrings) =>
          val relInfos = relInfoStrings flatMap RelInfo.fromString take (maxRelInfosReadPerArg) toSeq
          def notableRels = if (keepRelString) getNotableRels(relInfos) map (ri => (ri.string, ri.weight)) else Seq.empty // debug, deleteme
          val (topEntitiesForArg, totalEntityWeight) = typer.getTopEntitiesForArg(relInfos)
          val predictedTypes = typer.predictTypes(topEntitiesForArg)
          def typePrediction = TypePrediction(argString, predictedTypes, notableRels, totalEntityWeight, topEntitiesForArg.take(5).map(_.fbid))
          if (predictedTypes.isEmpty) None else Some(typePrediction.toString)
      }

      typePredictionStrings
    }

    
    val input: DList[String] = TextInput.fromTextFile(inputPath)
    
    val finalResult: DList[String] = {
      if (onlyPhaseOne) {
        this.configuration.jobNameIs("Unlinkable-Type-Prediction-PhaseOne-%s".format(argField.name))
        val argRelInfoPairs = phaseOne(input)
        argRelInfoPairs.map(pair => Seq(pair._1, pair._2).map(_.replaceAll("\t", "_TAB_")).mkString("\t"))
      } else if (onlyPhaseTwo) {
        // assume input is from phase one
        this.configuration.jobNameIs("Unlinkable-Type-Prediction-PhaseTwo-%s".format(argField.name))
        val argRelInfoPairs = input.map { line =>
          val split = tabSplit.split(line).take(2).map(_.replaceAll("_TAB_", "\t"))
          (split(0), split(1))
        }
        phaseTwo(argRelInfoPairs)
      } else {
        this.configuration.jobNameIs("Unlinkable-Type-Prediction-Full-%s".format(argField.name))
        phaseTwo(phaseOne(input))
      }
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
  
  private def dedupeSorted[T](input: Iterator[T]): Iterator[T] = {

    var last = Option.empty[T]
    def updateLast(t: T): Option[T] = { last = Some(t); last }
    input.flatMap { element =>
      last match {
        case None => updateLast(element)
        case Some(lastElement) => {
          if (element.equals(lastElement)) None
          else updateLast(element)
        }
      }
    }
  }
}

case class MutInt(var count: Long) { 
  def inc: Unit = { count += 1 } 
  def add(t: Long) = { count += t }
}
case object MutInt { def zero = MutInt(0) }

