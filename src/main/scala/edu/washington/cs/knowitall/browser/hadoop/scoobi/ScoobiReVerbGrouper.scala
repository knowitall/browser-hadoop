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
import edu.washington.cs.knowitall.browser.extraction.ArgEntity
import edu.washington.cs.knowitall.browser.extraction.ExtractionGroup
import edu.washington.cs.knowitall.browser.extraction.ReVerbExtractionGroup
import edu.washington.cs.knowitall.browser.hadoop.entity.TopCandidatesFinder
import edu.washington.cs.knowitall.browser.hadoop.entity.EntityLinker
import edu.washington.cs.knowitall.browser.hadoop.entity.Pair

import edu.washington.cs.knowitall.nlp.extraction.ChunkedExtraction

/**
 * A mapper + reducer job that
 * takes tab-delimited ReVerbExtractions as input, groups them by a normalization key, and
 * then constructs ExtractionGroup[ReVerbExtraction] from the reducer input. The Entity Linker
 * code is run in the reducer.
 */
class ScoobiReVerbGrouper(val stemmer: TaggedStemmer, val corpus: String) {

  var extrsProcessed = 0
  var groupsProcessed = 0
  
  case class RVTuple(arg1: String, rel: String, arg2: String) {
    override def toString = "%s__%s__%s".format(arg1, rel, arg2)
  }
  
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
    
    groupsProcessed += 1
    if (groupsProcessed % 10000 == 0) System.err.println("Groups processed: %d".format(groupsProcessed))

    def failure(msg: String = "") = {
      System.err.println("Error processing in processGroup: " + msg + ", key: " + key);
      rawExtrs.foreach(str => System.err.println(str))
      None
    }

    val extrs = rawExtrs.flatMap(line => ReVerbExtraction.fromTabDelimited(line.split("\t"))._1)
    		
    val head = extrs.head

    val origTuple = RVTuple(head.a1t, head.rt, head.a2t)
    val normTuple = getNormalizedKey(head)

    if (!normTuple.toString.equals(key)) return failure("Key Mismatch: " + normTuple.toString + " != " + key)

    val sources = extrs.map(e => e.source.getSentence().getTokensAsString()).toSeq

    val arg1Entity = None
    
    val arg2Entity = None

    val instances = extrs.map((_, corpus, None))

    val newGroup = new ExtractionGroup(
      normTuple.arg1,
      normTuple.rel,
      normTuple.arg2,
      arg1Entity,
      arg2Entity,
      Seq.empty[ArgEntity],
      Seq.empty[ArgEntity],
      instances.toSet[(ReVerbExtraction, String, Option[Double])])

    Some(newGroup)
  }

}

object ScoobiReVerbGrouper {
  
  val grouperCache = new mutable.HashMap[Thread, ScoobiReVerbGrouper]
  
  /** extrs --> grouped by normalization key */
  def groupExtractions(extrs: DList[String], corpus:String): DList[String] = {
    
    val keyValuePair: DList[(String, String)] = extrs.flatMap { line => 
      val grouper = grouperCache.getOrElseUpdate(Thread.currentThread, new ScoobiReVerbGrouper(TaggedStemmer.getInstance, corpus))
      grouper.getKeyValuePair(line) 
      }
    
    keyValuePair.groupByKey.flatMap {
      case (key, sources) =>
         val grouper = grouperCache.getOrElseUpdate(Thread.currentThread, new ScoobiReVerbGrouper(TaggedStemmer.getInstance, corpus))
        //val el = new EntityLinker
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
