package edu.washington.cs.knowitall.browser.hadoop.scoobi

import com.nicta.scoobi.Scoobi._
import com.nicta.scoobi.DList._
import com.nicta.scoobi.DList
import com.nicta.scoobi.io.text.TextInput._
import com.nicta.scoobi.io.text.TextInput
import com.nicta.scoobi.io.text.TextOutput._
import com.nicta.scoobi.io.text.TextOutput

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
import edu.washington.cs.knowitall.browser.extraction.ReVerbExtractionGroup
import edu.washington.cs.knowitall.browser.hadoop.entity.TopCandidatesFinder
import edu.washington.cs.knowitall.browser.hadoop.entity.EntityLinker
import edu.washington.cs.knowitall.browser.hadoop.entity.Pair

import edu.washington.cs.knowitall.nlp.extraction.ChunkedExtraction

  case class RVTuple(arg1: String, rel: String, arg2: String) {
    override def toString = "%s__%s__%s".format(arg1, rel, arg2)
  }

/**
 * A mapper + reducer job that
 * takes tab-delimited ReVerbExtractions as input, groups them by a normalization key, and
 * then constructs ExtractionGroup[ReVerbExtraction] from the reducer input. The Entity Linker
 * code is run in the reducer.
 */
class ScoobiReVerbGrouper(val stemmer: TaggedStemmer, val corpus: String) {

  
  
  var extrsProcessed = 0
  var groupsProcessed = 0
  
  var largestGroup = 0



  // returns an (arg1, rel, arg2) tuple of normalized string tokens
  def getNormalizedKey(extr: ReVerbExtraction): RVTuple = {
    def pairs(arg: ChunkedExtraction) = arg.getTokens.toSeq.zip(arg.getPosTags.toSeq)

    val arg1Pairs = pairs(extr.source.getArgument1).filter(!_._2.equals("DT"))
    val relPairs = pairs(extr.source.getRelation).filter(!_._2.equals("DT"))
    val arg2Pairs = pairs(extr.source.getArgument2).filter(!_._2.equals("DT"))

    val arg1Norm = stemmer.stemAll(arg1Pairs)
    val relNorm = stemmer.stemAll(relPairs)
    val arg2Norm = stemmer.stemAll(arg2Pairs)

    RVTuple(arg1Norm.mkString(" ").toLowerCase, relNorm.mkString(" ").toLowerCase, arg2Norm.mkString(" ").toLowerCase)
  }

  def getKeyValuePair(line: String): Option[(String, String)] = {

    extrsProcessed += 1
    if (extrsProcessed % 20000 == 0) System.err.println("Extractions processed: %d".format(extrsProcessed))

    // parse the line to a ReVerbExtraction
    val extrOpt = ReVerbExtraction.fromTabDelimited(line.split("\t"))._1

    extrOpt match {
      case Some(extr) => Some((getNormalizedKey(extr).toString, line))
      case None => None
    }
  }

  def processGroup(key: String, rawExtrs: Iterable[String]): Option[ExtractionGroup[ReVerbExtraction]] = {

    val rawExtrsTruncated = rawExtrs.take(ScoobiReVerbGrouper.max_group_size)
    
    groupsProcessed += 1
    if (groupsProcessed % 10000 == 0) System.err.println("Groups processed: %d, current key: %s, largest group: %d".format(groupsProcessed, key, largestGroup))

    def failure(msg: String = "") = {
      System.err.println("Error in processGroup: " + msg + ", key: " + key);
      rawExtrsTruncated.foreach(str => System.err.println(str))
      None
    }

    val extrs = rawExtrsTruncated.flatMap(line => ReVerbExtraction.fromTabDelimited(line.split("\t"))._1)

    val head = extrs.head

    val normTuple = getNormalizedKey(head)

    if (!normTuple.toString.equals(key)) return failure("Key Mismatch: " + normTuple.toString + " != " + key)

    val sources = extrs.map(e => e.source.getSentence().getTokensAsString()).toSeq

    val arg1Entity = None

    val arg2Entity = None

    val instances = extrs.map(e => new Instance(e, corpus, None)).toSet

    if (instances.size > largestGroup) largestGroup = instances.size
    
    val newGroup = new ExtractionGroup(
      normTuple.arg1,
      normTuple.rel,
      normTuple.arg2,
      arg1Entity,
      arg2Entity,
      Seq.empty[FreeBaseType],
      Seq.empty[FreeBaseType],
      instances)

    Some(newGroup)
  }

}

object ScoobiReVerbGrouper {

  val max_group_size = 40000
  
  var calls = 0L
  val grouperCache = new mutable.HashMap[Thread, ScoobiReVerbGrouper] with mutable.SynchronizedMap[Thread, ScoobiReVerbGrouper]

  /** extrs --> grouped by normalization key */
  def groupExtractions(extrs: DList[String], corpus: String): DList[String] = {

    val keyValuePair: DList[(String, String)] = extrs.flatMap { line =>
      calls += 1
      if (calls % 10000 == 0) System.err.println("Grouper Cache size: " + grouperCache.size)
      val grouper = grouperCache.getOrElseUpdate(Thread.currentThread, new ScoobiReVerbGrouper(TaggedStemmer.getInstance, corpus))
      grouper.getKeyValuePair(line)
    }

    keyValuePair.groupByKey.flatMap {
      case (key, sources) =>
        calls += 1
        if (calls % 10000 == 0) System.err.println("Grouper Cache size: " + grouperCache.size)
        val grouper = grouperCache.getOrElseUpdate(Thread.currentThread, new ScoobiReVerbGrouper(TaggedStemmer.getInstance, corpus))
        grouper.processGroup(key, sources) match {

          case Some(group) => Some(ReVerbExtractionGroup.toTabDelimited(group))
          case None => None
        }
    }

  }

  def main(args: Array[String]) = withHadoopArgs(args) { a =>

    val (inputPath, outputPath, corpus) = (a(0), a(1), a(2))

    // serialized ReVerbExtractions
    val extrs: DList[String] = TextInput.fromTextFile(inputPath)

    val groups = groupExtractions(extrs, corpus)

    DList.persist(TextOutput.toTextFile(groups, outputPath + "/"));
  }
}
