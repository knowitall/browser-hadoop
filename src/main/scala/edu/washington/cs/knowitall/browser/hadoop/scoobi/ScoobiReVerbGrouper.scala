package edu.washington.cs.knowitall.browser.hadoop.scoobi

import com.nicta.scoobi.Scoobi._

import java.io.File
import java.io.FileWriter

import scala.util.Random
import scala.collection.JavaConversions._
import scala.collection.mutable

import edu.washington.cs.knowitall.common.Timing._
import edu.washington.cs.knowitall.browser.extraction.ReVerbExtraction
import edu.washington.cs.knowitall.browser.extraction.FreeBaseEntity
import edu.washington.cs.knowitall.browser.extraction.FreeBaseType
import edu.washington.cs.knowitall.browser.extraction.Instance
import edu.washington.cs.knowitall.browser.extraction.ExtractionGroup
import edu.washington.cs.knowitall.browser.util.TaggedStemmer
import edu.washington.cs.knowitall.browser.extraction.ReVerbExtractionGroup
import edu.washington.cs.knowitall.browser.entity.EntityLinker
import edu.washington.cs.knowitall.browser.entity.Pair

import edu.washington.cs.knowitall.nlp.extraction.ChunkedExtraction

/**
  * A mapper + reducer job that
  * takes tab-delimited ReVerbExtractions as input, groups them by a normalization key, and
  * then constructs ExtractionGroup[ReVerbExtraction] from the reducer input. The Entity Linker
  * code is run in the reducer.
  */
class ScoobiReVerbGrouper(val stemmer: TaggedStemmer, val corpus: String) {

  private var extrsProcessed = 0
  private var groupsProcessed = 0

  private var largestGroup = 0

  def getKeyValuePair(line: String): Option[(String, String)] = try {

    extrsProcessed += 1
    if (extrsProcessed % 20000 == 0) System.err.println("Extractions processed: %d".format(extrsProcessed))

    // parse the line to a ReVerbExtraction
    val extrOpt = ReVerbExtraction.deserializeFromString(line)

    extrOpt match {
      case Some(extr) => {
        val key = extr.indexGroupingKeyString
        Some((key, line))
      }
      case None => None
    }
  } catch {
    case e: Exception => { e.printStackTrace; None }
  }

  def processGroup(key: String, rawExtrs: Iterable[String]): Option[ExtractionGroup[ReVerbExtraction]] = try {

    val rawExtrsTruncated = rawExtrs.take(ReVerbGrouperStaticVars.max_group_size)

    groupsProcessed += 1
    if (groupsProcessed % 10000 == 0) System.err.println("Groups processed: %d, current key: %s, largest group: %d".format(groupsProcessed, key, largestGroup))

    val extrs = rawExtrsTruncated.flatMap(line => ReVerbExtraction.deserializeFromString(line))

    val head = extrs.head

    val normKey = head.indexGroupingKeyString
    val normTuple = head.indexGroupingKey

    require(normKey.equals(key))

    val sources = extrs.map(e => e.sentenceTokens.map(_.string).mkString(" "))

    val arg1Entity = None

    val arg2Entity = None

    // this line also computes confidences for each instance, in tryAddConf...
    val instances = extrs.map(e => ScoobiGroupReGrouper.tryAddConf(new Instance(e, corpus, -1.0))).toSet

    if (instances.size > largestGroup) largestGroup = instances.size

    val newGroup = new ExtractionGroup(
      normTuple._1,
      normTuple._2,
      normTuple._3,
      arg1Entity,
      arg2Entity,
      Set.empty[FreeBaseType],
      Set.empty[FreeBaseType],
      instances)

    Some(newGroup)
  } catch {
    case e: Exception => {System.err.println("empty list!"); e.printStackTrace; None }
  }
}

object ReVerbGrouperStaticVars {
  val grouperCache = new mutable.HashMap[Thread, ScoobiReVerbGrouper] with mutable.SynchronizedMap[Thread, ScoobiReVerbGrouper]
  val max_group_size = 40000
}

object ScoobiReVerbGrouper extends ScoobiApp {
  import ReVerbGrouperStaticVars._

  var calls = 0L

  /** extrs --> grouped by normalization key */
  def groupExtractions(extrs: DList[String], corpus: String): DList[String] = {

    val keyValuePair: DList[(String, String)] = extrs.flatMap { line =>
      calls += 1
      if (calls % 10000 == 0) System.err.println("Grouper Cache size: " + grouperCache.size)
      val grouper = grouperCache.getOrElseUpdate(Thread.currentThread, new ScoobiReVerbGrouper(TaggedStemmer.instance, corpus))
      grouper.getKeyValuePair(line)
    }

    keyValuePair.groupByKey.flatMap {
      case (key, sources) =>
        calls += 1
        if (calls % 10000 == 0) System.err.println("Grouper Cache size: " + grouperCache.size)
        val grouper = grouperCache.getOrElseUpdate(Thread.currentThread, new ScoobiReVerbGrouper(TaggedStemmer.instance, corpus))
        grouper.processGroup(key, sources) match {
          case Some(group) => Some(ReVerbExtractionGroup.serializeToString(group))
          case None => None
        }
    }

  }

  def run() = {

    val (inputPath, outputPath, corpus) = (args(0), args(1), args(2))

    // serialized ReVerbExtractions
    val extrs: DList[String] = fromTextFile(inputPath)

    val groups = groupExtractions(extrs, corpus)

    persist(TextOutput.toTextFile(groups, outputPath + "/"));
  }
}
