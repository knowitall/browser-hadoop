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
import edu.washington.cs.knowitall.browser.extraction.ExtractionGroup
import edu.washington.cs.knowitall.browser.extraction.Instance
import edu.washington.cs.knowitall.browser.extraction.ReVerbExtractionGroup
import edu.washington.cs.knowitall.browser.util.TaggedStemmer
import edu.washington.cs.knowitall.browser.hadoop.entity.TopCandidatesFinder
import edu.washington.cs.knowitall.browser.hadoop.entity.EntityLinker
import edu.washington.cs.knowitall.browser.hadoop.entity.Pair
import edu.washington.cs.knowitall.nlp.extraction.ChunkedArgumentExtraction

import edu.washington.cs.knowitall.nlp.extraction.ChunkedExtraction

/**
 * A mapper job that
 * takes tab-delimited ReVerbExtractions as input, groups them by a normalization key, and
 * then constructs ExtractionGroup[ReVerbExtraction] from the reducer input.
 * linkers is a Seq --- this is because each points to a different lucene index on one of the four of
 * reliable's scratch disks, which helps balance the load, allowing you to run more of these 
 * as hadoop map tasks
 */
class ScoobiEntityLinker(val linkers: Seq[EntityLinker], val stemmer: TaggedStemmer) {
  
  import ScoobiEntityLinker.getRandomElement
  
  private var groupsProcessed = 0
  private var arg1sLinked = 0
  private var arg2sLinked = 0

  private var hcArg1sLinked = 0
  private var hcArg2sLinked = 0
 
  private var hcArg1sTotal = 0
  private var hcArg2sTotal = 0
  
  def getEntity(el: EntityLinker, arg: String, head: ReVerbExtraction, sources: Set[String]): Option[FreeBaseEntity] = {
    
    val tryEL = el.getBestFbidFromSources(arg, sources)

    if (tryEL != null) {

      Some(FreeBaseEntity(tryEL.one, tryEL.two))
    }
    else None
  }
  
  def linkEntities(group: ExtractionGroup[ReVerbExtraction]): ExtractionGroup[ReVerbExtraction] = {

    groupsProcessed += 1
    
    val extrs = group.instances.map(_.extraction)

    val head = extrs.head
      
    val sources = extrs.map(e => e.sentenceTokens.map(_.string).mkString(" "))
    // choose a random linker to distribute the load more evenly across the cluster
    val randomLinker = getRandomElement(linkers)
    val arg1Entity = if (group.arg1Entity.isDefined) group.arg1Entity else getEntity(randomLinker, head.arg1Tokens, head, sources)

    if (arg1Entity.isDefined) {
      arg1sLinked += 1
    }
    
    val arg2Entity = if (group.arg2Entity.isDefined) group.arg2Entity else getEntity(randomLinker, head.arg2Tokens, head, sources)
    
    if (arg2Entity.isDefined) {
      arg2sLinked += 1
    }
    
    
    val newGroup = new ExtractionGroup(
      group.arg1Norm,
      group.relNorm,
      group.arg2Norm,
      arg1Entity,
      arg2Entity,
      group.arg1Types,
      group.arg2Types,
      group.instances.map(inst => new Instance(inst.extraction, inst.corpus, inst.confidence)))

    if (groupsProcessed % 10000 == 0) System.err.println("Groups processed: %d, Arg1s Linked: %d, Arg2s Linked: %d".format(groupsProcessed, arg1sLinked, arg2sLinked))
    
    newGroup
  }

}

object ScoobiEntityLinker {
  
  // std. deviation for the wait times
  val max_init_wait_ms = 1 * 1 * 1000;
  
  val random = new scala.util.Random

  // hardcoded for the rv cluster - the location of Tom's freebase context similarity index.
  // Indexes are on the /scratchX/ where X in {"", 2, 3, 4}, the method getScratch currently
  // decides how to pick one of the choices.
  val baseIndex = "browser-freebase/3-context-sim/index"

  //val linkerCache = new mutable.HashMap[Thread, ScoobiEntityLinker] with mutable.SynchronizedMap[Thread, ScoobiEntityLinker]
  val linkers = new ThreadLocal[ScoobiEntityLinker] { override def initialValue = delayedInitEntityLinker }
    
  /** Get a random scratch directory on an RV node. */
  def getScratch(pathAfterScratch: String): Seq[String] = {

    for (i <- 1 to 4) yield {
      val numStr = if (i == 1) "" else i.toString
      "/scratch%s/".format(i)+pathAfterScratch
    }
  }

  def delayedInitEntityLinker = {
    
    // wait for a random period of time
    val randWaitMs = math.abs(random.nextGaussian) * max_init_wait_ms
    System.err.println("Delaying %.02f seconds before initializing..".format(randWaitMs / 1000.0))
    
    Thread.sleep(randWaitMs.toInt)
        
    val el = getScratch(baseIndex).map(index=>new EntityLinker(index))
    new ScoobiEntityLinker(el, TaggedStemmer.getInstance)
  }
   
  def getRandomElement[T](seq: Seq[T]): T = seq(Random.nextInt(seq.size))
  
  def linkGroups(groups: DList[String]): DList[String] = {
    
    groups.flatMap { line =>
      val scoobiLinker = linkers.get
      val extrOp = ReVerbExtractionGroup.fromTabDelimited(line.split("\t"))._1
      extrOp match {
        case Some(extr) => Some(ReVerbExtractionGroup.toTabDelimited(scoobiLinker.linkEntities(extr)))
        case None => None
      }
    }
  }
  
  def main(args: Array[String]) = withHadoopArgs(args) { remainingArgs =>

    conf.set("mapred.job.name", "browser entity linker")

    val (inputPath, outputPath) = (remainingArgs(0), remainingArgs(1))

    val lines: DList[String] = TextInput.fromTextFile(inputPath)

    val linkedGroups: DList[String] = linkGroups(lines) 

    DList.persist(TextOutput.toTextFile(linkedGroups, outputPath + "/"));
  }
}
