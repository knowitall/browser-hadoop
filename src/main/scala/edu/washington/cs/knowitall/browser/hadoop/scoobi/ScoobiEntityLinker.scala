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
import edu.washington.cs.knowitall.nlp.extraction.ChunkedArgumentExtraction

import edu.washington.cs.knowitall.nlp.extraction.ChunkedExtraction

/**
 * A mapper + reducer job that
 * takes tab-delimited ReVerbExtractions as input, groups them by a normalization key, and
 * then constructs ExtractionGroup[ReVerbExtraction] from the reducer input. The Entity Linker
 * code is run in the reducer.
 */
class ScoobiEntityLinker(val stemmer: TaggedStemmer) {

  val min_support_sentences = 2
  
  def getEntity(el: EntityLinker, arg: String, head: ReVerbExtraction, sources: Seq[String]): Option[ArgEntity] = {

    val tryEL = el.getBestFbidFromSources(arg, sources)

    if (tryEL != null) Some(ArgEntity(tryEL.one, tryEL.two))
    else None
  }

  def linkEntities(el: EntityLinker, group: ExtractionGroup[ReVerbExtraction]): ExtractionGroup[ReVerbExtraction] = {

    val extrs = group.instances.map(_._1)

    val head = extrs.head

    val sources = extrs.map(e => e.source.getSentence().getTokensAsString()).toSeq

    val enoughSources = extrs.size >= min_support_sentences
    
    def hasProper(arg: ChunkedArgumentExtraction) = arg.getPosTags.exists(_.startsWith("NNP"))
    
    val arg1Entity = if (enoughSources && hasProper(head.source.getArgument1())) {
      getEntity(el, head.arg1Tokens, head, sources)
    } else {
      None
    }

    val arg2Entity = if (enoughSources && hasProper(head.source.getArgument2())) {
      getEntity(el, head.arg2Tokens, head, sources)
    } else {
      None
    }

    val newGroup = new ExtractionGroup(
      group.arg1Norm,
      group.relNorm,
      group.arg2Norm,
      arg1Entity,
      arg2Entity,
      group.arg1Types,
      group.arg2Types,
      group.instances.map(tup => (tup._1, "g1b", tup._3)))

    newGroup
  }

}

object ScoobiEntityLinker {

  val max_init_wait_ms = 3 * 60 * 1000;
  
  val random = new scala.util.Random

  // hardcoded for the rv cluster - the location of Tom's freebase context similarity index.
  val baseIndex = /*/scratch*/ "browser-freebase/3-context-sim/index"

  val linkerCache = new mutable.HashMap[Thread, EntityLinker]

  /** Get a random scratch directory on an RV node. */
  def getScratch: String = {

    try {
      val num = (scala.math.abs(random.nextInt) % 4) + 1
      if (num == 1) return "/scratch/"
      else return "/scratch%s/".format(num.toString)
    } catch {
      case e: NumberFormatException => {
        e.printStackTrace()
        System.err.println("Error getting index")
        return "/scratch/"
      }
    }
  }

  def delayedInitEntityLinker = {
    
    // wait for a random period of time
    val randWaitMs = random.nextInt(max_init_wait_ms)
    System.err.println("Delaying %.02f seconds before initializing..".format(randWaitMs.toDouble / 1000.0))
    
    Thread.sleep(randWaitMs)
        
    new EntityLinker(getScratch + baseIndex)
  }
  
  def main(args: Array[String]) = withHadoopArgs(args) { remainingArgs =>

    val flink = new ScoobiEntityLinker(TaggedStemmer.getInstance)

    val (inputPath, outputPath) = (remainingArgs(0), remainingArgs(1))

    val lines: DList[String] = TextInput.fromTextFile(inputPath)

    val linkedGroups: DList[String] = lines.flatMap { line =>
      val el = linkerCache.getOrElseUpdate(Thread.currentThread(), delayedInitEntityLinker)
      val extrOp = ReVerbExtractionGroup.fromTabDelimited(line.split("\t"))._1
      extrOp match {
        case Some(extr) => Some(ReVerbExtractionGroup.toTabDelimited(flink.linkEntities(el, extr)))
        case None => None
      }
    }

    DList.persist(TextOutput.toTextFile(linkedGroups, outputPath + "/"));
  }
}
