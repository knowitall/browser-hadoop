package edu.washington.cs.knowitall.browser.hadoop.scoobi

import com.nicta.scoobi.Scoobi._
import com.nicta.scoobi.DList._
import com.nicta.scoobi.DList
import com.nicta.scoobi.io.text.TextInput._
import com.nicta.scoobi.io.text.TextInput
import com.nicta.scoobi.io.text.TextOutput._
import com.nicta.scoobi.io.text.TextOutput

import edu.washington.cs.knowitall.common.Timing._
import edu.washington.cs.knowitall.browser.extraction.ReVerbExtraction
import edu.washington.cs.knowitall.browser.extraction.FreeBaseEntity
import edu.washington.cs.knowitall.browser.extraction.FreeBaseType
import edu.washington.cs.knowitall.browser.extraction.Instance
import edu.washington.cs.knowitall.browser.extraction.ExtractionGroup
import edu.washington.cs.knowitall.browser.util.TaggedStemmer
import edu.washington.cs.knowitall.browser.extraction.ReVerbExtractionGroup

import scopt.OptionParser

object ScoobiInlinkRatioComputer {
  
  private val NO_ENTITY = "*NO_ENTITY*"
  
  type REG = ExtractionGroup[ReVerbExtraction]
  
  def main(args: Array[String]): Unit = withHadoopArgs(args) { a =>

    var inputPath, outputPath = ""
    var processArg1 = true

    val parser = new OptionParser() {
      arg("inputPath", "hdfs input path, tab delimited ExtractionGroups with inlink counts", { str => inputPath = str })
      arg("outputPath", "hdfs output path, tab delimited ExtractionGroups with inlink counts", { str => outputPath = str })
      arg("processArg1", "arg1 for arg1, anything else for arg2", { str => processArg1 = str.equals("arg1") })
    }

    if (!parser.parse(args)) return
    
    // serialized ReVerbExtractions
    val lines: DList[String] = TextInput.fromTextFile(inputPath)

    val groups = lines.flatMap { line =>
      ReVerbExtractionGroup.fromTabDelimited(line.split("\t"))._1 match {
        case Some(extrGroup) => Some((extrGroup, line))
        case None => None
      }
    }

    val argKeyValuePairs = groups.map {
      case (group, line) =>
        if (processArg1) {
          group.arg1Entity match {
            case Some(entity) => (entity.name, line)
            case None => ("*NO_ENTITY*", line)
          }
        } else {
          group.arg2Entity match {
            case Some(entity) => (entity.name, line)
            case None => ("*NO_ENTITY*", line)
          }
        }
    }

    val argGrouped = argKeyValuePairs.groupByKey
    
    val argsFinished = argGrouped.flatMap { case (key, extrGroups) => 
      if (!key.equals(NO_ENTITY)) processReducerGroup(processArg1, extrGroups) else extrGroups 
    }
    
    DList.persist(TextOutput.toTextFile(argsFinished, outputPath + "/"));
  }
  
  /**
   * Assumes all REGs are linked, don't call this if there isn't a link in given arg field.
   */
  def processReducerGroup(arg1: Boolean, rawExtrGroups: Iterable[String]): Iterable[String] = {
    
    val extrGroups = rawExtrGroups.flatMap(line=>ReVerbExtractionGroup.fromTabDelimited(line.split("\t"))._1)
    
    val arg2 = !arg1
    val size = extrGroups.size
    extrGroups.map { extrGroup =>
      
      val procEntity = if (arg1) extrGroup.arg1Entity.get else extrGroup.arg2Entity.get
      val inlinks = procEntity.inlinkRatio
      val ratio = size.toDouble / inlinks.toDouble
      val arg1Entity = if (arg1) Some(new FreeBaseEntity(procEntity.name, procEntity.fbid, procEntity.score, ratio)) else extrGroup.arg1Entity
      val arg2Entity = if (arg2) Some(new FreeBaseEntity(procEntity.name, procEntity.fbid, procEntity.score, ratio)) else extrGroup.arg2Entity 
      
      new ExtractionGroup(
          extrGroup.arg1Norm,
          extrGroup.relNorm,
          extrGroup.arg2Norm,
          arg1Entity,
          arg2Entity,
          extrGroup.arg1Types,
          extrGroup.arg2Types,
          extrGroup.instances
      )
    }.map (ReVerbExtractionGroup.toTabDelimited(_))
  }
  
}