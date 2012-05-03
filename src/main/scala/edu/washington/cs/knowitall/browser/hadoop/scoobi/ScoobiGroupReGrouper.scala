package edu.washington.cs.knowitall.browser.hadoop.scoobi

import com.nicta.scoobi.Scoobi._
import com.nicta.scoobi.DList._
import com.nicta.scoobi.DList
import com.nicta.scoobi.io.text.TextInput._
import com.nicta.scoobi.io.text.TextInput
import com.nicta.scoobi.io.text.TextOutput._
import com.nicta.scoobi.io.text.TextOutput

import edu.washington.cs.knowitall.browser.extraction.ReVerbExtraction
import edu.washington.cs.knowitall.browser.extraction.ExtractionGroup
import edu.washington.cs.knowitall.browser.extraction.Instance
import edu.washington.cs.knowitall.browser.extraction.ReVerbExtractionGroup

import edu.washington.cs.knowitall.extractor.conf.ReVerbConfFunction
import edu.washington.cs.knowitall.nlp.extraction.ChunkedExtraction

object ScoobiGroupReGrouper {
  
  val confLocal = new ThreadLocal[ReVerbConfFunction]() {
    override def initialValue = new ReVerbConfFunction()
  }

  var extrsProcessed = 0
  
  var groupsProcessed = 0

  def main(args: Array[String]) = withHadoopArgs(args) { a =>

    val (inputPath, outputPath) = (a(0), a(1))

    // serialized ReVerbExtractions
    val groups: DList[String] = TextInput.fromTextFile(inputPath)

    val confedGroups = groups.flatMap { line => groupMapProcessor(line) }
    
    val reGroups = confedGroups.map { group => getKeyValuePair(group) }.groupByKey
    
    val combinedGroups = reGroups.map(keyValues => ReVerbExtractionGroup.toTabDelimited(combineGroups(keyValues._1, keyValues._2)))

    DList.persist(TextOutput.toTextFile(combinedGroups, outputPath + "/"));
  }

  def combineGroups(key: String, groups: Iterable[String]): ExtractionGroup[ReVerbExtraction] = {
    
    
    val parsedGroups = groups.flatMap(str => ReVerbExtractionGroup.fromTabDelimited(str.split("\t"))._1)
    
    val allInstances = parsedGroups.flatMap { group =>
        val keyCheck = getKeyValuePair(group)._1
        if (!keyCheck.equals(key)) System.err.println("Key mismatch, found %s expected %s".format(keyCheck, key))
        group.instances
    }
    
    val head = parsedGroups.head
    
    val combinedGroup = new ExtractionGroup(head.arg1Norm,
        head.relNorm,
        head.arg2Norm,
        head.arg1Entity,
        head.arg2Entity,
        head.arg1Types,
        head.arg2Types,
        allInstances.take(ScoobiReVerbGrouper.max_group_size).toSet)
    
    groupsProcessed += parsedGroups.size
    if (groupsProcessed % 10000 == 0) System.err.println("Groups combined: %d".format(groupsProcessed))
    
    combinedGroup
  }
  
  def getKeyValuePair(group: ExtractionGroup[ReVerbExtraction]): (String, String) = {

    extrsProcessed += 1
    if (extrsProcessed % 20000 == 0) System.err.println("Extractions processed: %d".format(extrsProcessed))

    val normTuple = RVTuple(group.arg1Norm, group.relNorm, group.arg2Norm)
    (normTuple.toString, ReVerbExtractionGroup.toTabDelimited(group))

  }

  def groupMapProcessor(line: String): Option[ExtractionGroup[ReVerbExtraction]] = {

    // try to parse the line into a group
    val group = ReVerbExtractionGroup.fromTabDelimited(line.split("\t"))._1.getOrElse { return None }

    // go through and assign confs to any extractions without them
    val confedInstances = group.instances.map { inst =>
      inst.confidence match {
        case Some(conf) => inst
        case None => tryAddConf(inst)
      }
    }

    val newGroup = new ExtractionGroup(group.arg1Norm,
      group.relNorm,
      group.arg2Norm,
      group.arg1Entity,
      group.arg2Entity,
      group.arg1Types,
      group.arg2Types,
      confedInstances)

    Some(newGroup)
  }

  /**
    * Tries to attach a conf to inst, if it doesn't already have one. If it fails, reports an error, but returns inst unchanged.
    */
  private def tryAddConf(inst: Instance[ReVerbExtraction]): Instance[ReVerbExtraction] = {
    try {
      val conf = confLocal.get().getConf(inst.extraction.source)
      new Instance(inst.extraction, inst.corpus, Some(conf))
    } catch {
      case e: Exception => { e.printStackTrace; System.err.println(inst.extraction.source); inst }
    }
  }

}