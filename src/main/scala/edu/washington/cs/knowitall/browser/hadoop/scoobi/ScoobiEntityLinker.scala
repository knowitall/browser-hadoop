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
import edu.washington.cs.knowitall.browser.extraction.ExtractionGroup
import edu.washington.cs.knowitall.browser.extraction.ReVerbExtractionGroup
import edu.washington.cs.knowitall.browser.hadoop.entity.TopCandidatesFinder
import edu.washington.cs.knowitall.browser.hadoop.entity.EntityLinker
import edu.washington.cs.knowitall.browser.hadoop.entity.Pair

import edu.washington.cs.knowitall.nlp.extraction.ChunkedExtraction

class ScoobiEntityLinker(val stemmer: TaggedStemmer) {

  case class RVTuple(arg1: String, rel: String, arg2: String) {
    override def toString = "%s, %s, %s".format(arg1, rel, arg2)
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
    
    
    RVTuple(arg1Norm.mkString(" "), relNorm.mkString(" "), arg2Norm.mkString(" "))
  }

  def getKeyValuePair(line: String): Option[(String, String)] = {
    // parse the line to a ReVerbExtraction
    val extrOpt = ReVerbExtraction.fromTabDelimited(line.split("\t"))._1

    extrOpt match {
      case Some(extr) => Some((getNormalizedKey(extr).toString, line))
      case None => None
    }
  }

  def getEntity(el: EntityLinker, arg: String, head: ReVerbExtraction, sources: Seq[String]): Option[(String, String)] = {

       
    var argEntity: Option[(String, String)] = None

    val tryEL = el.getBestFbidFromSources(arg, sources)
    if (tryEL != null) argEntity = Some((tryEL.one, tryEL.two))
    argEntity
  }

  def processGroup(el: EntityLinker, key: String, rawExtrs: Iterable[String]): Option[ExtractionGroup[ReVerbExtraction]] = {

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

    val arg1Entity = if (head.source.getArgument1().getPosTags().exists(_.startsWith("NNP"))) {
      getEntity(el, head.arg1Tokens, head, sources).map(_._1)
    } else {
      None
    }
    
    val arg2Entity = if (head.source.getArgument2().getPosTags().exists(_.startsWith("NNP"))) {
      getEntity(el, head.arg2Tokens, head, sources).map(_._1)
    } else {
      None
    }

    val instances = extrs.map((_, "TeST", None))

    val newGroup = new ExtractionGroup(
      normTuple.arg1,
      normTuple.rel,
      normTuple.arg2,
      arg1Entity,
      arg2Entity,
      None,
      None,
      instances)

    Some(newGroup)
  }

}

object ScoobiEntityLinker {

  val linkerCache = new mutable.HashMap[Thread, EntityLinker]
  
  def main(args: Array[String]) = withHadoopArgs(args) { a =>

    val flink = new ScoobiEntityLinker(TaggedStemmer.getInstance)

    val (inputPath, outputPath) = (a(0), a(1))

    val lines: DList[String] = TextInput.fromTextFile(inputPath)

    // first we need to define what will be our key. We strip only articles - everything else just gets normalized... 
    // normalized by something that takes POS tags, so it performs best.
    val keyValuePair: DList[(String, String)] = lines.flatMap { line => flink.getKeyValuePair(line) }

    val groups = keyValuePair.groupByKey.flatMap {
      case (key, sources) =>
        //val el = new EntityLinker
        val el = linkerCache.getOrElseUpdate(Thread.currentThread(), new EntityLinker())
        val (t, result) = time {
          flink.processGroup(el, key, sources) match {
            case Some(group) => Some(ReVerbExtractionGroup.toTabDelimited(group))
            case None => None
          }
        }

        if (Random.nextDouble < 0.0002) {
          System.err.println("Group processing time: %s for key %s, group:".format(Milliseconds.format(t), key))
          System.err.println(if (result.isDefined) result.get else "None")
          System.err.println("Linker instance cache: "+linkerCache.toString)
        }

        result
    }

    DList.persist(TextOutput.toTextFile(groups, outputPath + "/"));
  }
}
