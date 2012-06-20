package edu.washington.cs.knowitall.browser.hadoop.scoobi

import com.nicta.scoobi.Scoobi._
import com.nicta.scoobi.DList._
import com.nicta.scoobi.DList
import com.nicta.scoobi.io.text.TextInput._
import com.nicta.scoobi.io.text.TextInput
import com.nicta.scoobi.io.text.TextOutput._
import com.nicta.scoobi.io.text.TextOutput

import java.net.InetSocketAddress

import java.io.File
import java.io.FileWriter

import scala.util.Random
import scala.collection.JavaConversions._
import scala.collection.mutable

import edu.washington.cs.knowitall.common.Timing._
import edu.washington.cs.knowitall.browser.extraction.ReVerbExtraction
import edu.washington.cs.knowitall.browser.extraction.FreeBaseEntity
import edu.washington.cs.knowitall.browser.extraction.FreeBaseType
import edu.washington.cs.knowitall.browser.extraction.ExtractionGroup
import edu.washington.cs.knowitall.browser.extraction.Instance
import edu.washington.cs.knowitall.browser.extraction.ReVerbExtractionGroup
import edu.washington.cs.knowitall.browser.util.TaggedStemmer
import edu.washington.cs.knowitall.browser.entity.TopCandidatesFinder
import edu.washington.cs.knowitall.browser.entity.EntityLinker
import edu.washington.cs.knowitall.browser.entity.Pair
import edu.washington.cs.knowitall.browser.entity.Entity
import edu.washington.cs.knowitall.nlp.extraction.ChunkedArgumentExtraction

import edu.washington.cs.knowitall.nlp.extraction.ChunkedExtraction

import edu.washington.cs.knowitall.browser.entity.EntityLinker

import scopt.OptionParser

/**
  * A mapper job that
  * takes tab-delimited ReVerbExtractions as input, groups them by a normalization key, and
  * then constructs ExtractionGroup[ReVerbExtraction] from the reducer input.
  * linkers is a Seq --- this is because each points to a different lucene index on one of the four of
  * reliable's scratch disks, which helps balance the load, allowing you to run more of these
  * as hadoop map tasks
  *
  * Also adds types - entityTyper does not have to be run as a separate job
  */
class ScoobiEntityLinker(val subLinkers: Seq[EntityLinker], val stemmer: TaggedStemmer) {

  import ScoobiEntityLinker.getRandomElement
  import ScoobiEntityLinker.min_arg_length

  private val scoobiTyper = new ScoobiEntityTyper()
  
  private var groupsProcessed = 0
  private var arg1sLinked = 0
  private var arg2sLinked = 0
  private var totalGroups = 0
  
  def getEntity(el: EntityLinker, arg: String, head: ReVerbExtraction, sources: Set[String]): Option[Entity] = {

    if (arg.length < min_arg_length) None

    val tryLink = el.getBestEntity(arg, sources.toSeq)

    if (tryLink == null) None else Some(tryLink)
  }
  
  def entityConversion(entity: Entity): (Option[FreeBaseEntity], Set[FreeBaseType]) = {
    
    val fbEntity = FreeBaseEntity(entity.name, entity.fbid, entity.score, entity.inlinks)
    
    val fbTypes = entity.retrieveTypes flatMap FreeBaseType.parse toSet
    
    (Some(fbEntity), fbTypes)
  }

  def linkEntities(group: ExtractionGroup[ReVerbExtraction], reuseLinks: Boolean): ExtractionGroup[ReVerbExtraction] = {

    // a hack for the thread problem
    if (groupsProcessed == 0) {
      val keys = Thread.getAllStackTraces.keySet
      System.err.println("Num threads running: " + keys.size)
      keys.foreach { thread => System.err.println("%s, %s, %s".format(thread.getId, thread.getName, thread.getPriority)) }
    }
    
    groupsProcessed += 1

    val extrs = group.instances.map(_.extraction)

    val head = extrs.head

    val sources = extrs.map(e => e.sentenceTokens.map(_.string).mkString(" "))
    // choose a random linker to distribute the load more evenly across the cluster
    val randomLinker = getRandomElement(subLinkers)
    
    val (arg1Entity, arg1Types) = if (reuseLinks && group.arg1.entity.isDefined) {
      (group.arg1.entity, group.arg1.types) 
    } else {
      getEntity(randomLinker, head.arg1Text, head, sources) match {
        case Some(rawEntity) => { arg1sLinked += 1; entityConversion(rawEntity) }
        case None => (Option.empty[FreeBaseEntity], Set.empty[FreeBaseType])
      }
    }
    


    val (arg2Entity, arg2Types) = if (reuseLinks && group.arg2.entity.isDefined) {
      (group.arg2.entity, group.arg2.types) 
    } else {
      getEntity(randomLinker, head.arg2Text, head, sources) match {
        case Some(rawEntity) => { arg2sLinked += 1; entityConversion(rawEntity) }
        case None => (Option.empty[FreeBaseEntity], Set.empty[FreeBaseType])
      }
    }

    val newGroup = new ExtractionGroup(
      group.arg1.norm,
      group.rel.norm,
      group.arg2.norm,
      arg1Entity,
      arg2Entity,
      arg1Types,
      arg2Types,
      group.instances.map(inst => new Instance(inst.extraction, inst.corpus, inst.confidence)))
    
    newGroup
  }
}

object ScoobiEntityLinker {

  val cachePort = 11211
  
  val cacheNodes = {
    val localhost = java.net.InetAddress.getLocalHost.getHostName
    Seq(new InetSocketAddress(localhost, cachePort)) 
  }
                          
  private val min_arg_length = 3
  val linkersLocal = new mutable.HashMap[Thread, ScoobiEntityLinker] with mutable.SynchronizedMap[Thread, ScoobiEntityLinker]

  val random = new scala.util.Random

  // hardcoded for the rv cluster - the location of Tom's freebase context similarity index.
  // Indexes are on the /scratchX/ where X in {"", 2, 3, 4}, the method getScratch currently
  // decides how to pick one of the choices.
  val baseIndex = "browser-freebase/3-context-sim/index"

  /** Get a random scratch directory on an RV node. */
  def getScratch(pathAfterScratch: String): Seq[String] = {

    for (i <- 1 to 4) yield {
      val numStr = if (i == 1) "" else i.toString
      "/scratch%s/".format(numStr) + pathAfterScratch
    }
  }

  case class Counter(var count: Int) { def inc(): Unit = { count += 1 } }
  val counterLocal = new ThreadLocal[Counter]() { override def initialValue = Counter(0) }

  def getRandomElement[T](seq: Seq[T]): T = seq(Random.nextInt(seq.size))

  def getEntityLinker = {
    val el = getScratch(baseIndex).map(index => new EntityLinker(index)) // java doesn't have Option
    new ScoobiEntityLinker(el, TaggedStemmer.threadLocalInstance)
  }

  def linkGroups(groups: DList[String], minFreq: Int, maxFreq: Int, reportInterval: Int, skipLinking: Boolean): DList[String] = {

    if (skipLinking) return frequencyFilter(groups, minFreq, maxFreq, reportInterval, skipLinking)
    
    groups.flatMap { line =>
      val counter = counterLocal.get
      counter.inc
      val linker = linkersLocal.getOrElseUpdate(Thread.currentThread, getEntityLinker)
      if (counter.count % reportInterval == 0) {
        val format = "MinFreq: %d, MaxFreq: %d, groups input: %d, groups output: %d, arg1 links: %d, arg2 links: %d"
        System.err.println(format.format(minFreq, maxFreq, counter.count, linker.groupsProcessed, linker.arg1sLinked, linker.arg2sLinked))
      }

      val extrOp = ReVerbExtractionGroup.fromTabDelimited(line.split("\t"))._1
      extrOp match {
        case Some(extr) => {
          if (extr.instances.size <= maxFreq && extr.instances.size >= minFreq) {
            Some(ReVerbExtractionGroup.toTabDelimited(linker.linkEntities(extr, reuseLinks = true)))
          } else {
            None
          }
        }
        case None => { System.err.println("ScoobiEntityLinker: Error parsing a group: %s".format(line)); None }
      }
    }
  }
  
  
  def frequencyFilter(groups: DList[String], minFreq: Int, maxFreq: Int, reportInterval: Int, skipLinking: Boolean): DList[String] = {

    var groupsOutput = 0
    
    groups.flatMap { line =>
      val counter = counterLocal.get
      counter.inc
      if (counter.count % reportInterval == 0) {
        val format = "(Skipping Linking) MinFreq: %d, MaxFreq: %d, groups input: %d, groups output: %d"
        System.err.println(format.format(minFreq, maxFreq, counter.count, groupsOutput))
      }

      val extrOp = ReVerbExtractionGroup.fromTabDelimited(line.split("\t"))._1
      extrOp match {
        case Some(extr) => {
          if (extr.instances.size <= maxFreq && extr.instances.size >= minFreq) {
            groupsOutput += 1
            Some(ReVerbExtractionGroup.toTabDelimited(extr))
          } else {
            None
          }
        }
        case None => { System.err.println("ScoobiEntityLinker: Error parsing a group: %s".format(line)); None }
      }
    }
  }

  def main(args: Array[String]) = withHadoopArgs(args) { remainingArgs =>

    var minFreq = 0
    var maxFreq = scala.Int.MaxValue
    var inputPath, outputPath = ""
    var reportInterval = 20000
    var skipLinking = false;

    val parser = new OptionParser() {
      arg("inputPath", "hdfs input path, tab delimited ExtractionGroups", { str => inputPath = str })
      arg("outputPath", "hdfs output path, tab delimited ExtractionGroups", { str => outputPath = str })
      opt("minFreq", "minimum num instances in a group to process it inclusive default 0", { str => minFreq = str.toInt })
      opt("maxFreq", "maximum num instances in a group to process it inclusive default Int.MaxValue", { str => maxFreq = str.toInt })
      opt("reportInterval", "print simple stats every n input groups default 20000", { str => reportInterval = str.toInt })
      opt("skipLinking", "don't ever actually try to link - (use for frequency filtering)", { skipLinking = true })
    }

    if (parser.parse(remainingArgs)) {

      val lines: DList[String] = TextInput.fromTextFile(inputPath)
      val linkedGroups: DList[String] = linkGroups(lines, minFreq, maxFreq, reportInterval, skipLinking)

      DList.persist(TextOutput.toTextFile(linkedGroups, outputPath + "/"));
    }
  }
}
