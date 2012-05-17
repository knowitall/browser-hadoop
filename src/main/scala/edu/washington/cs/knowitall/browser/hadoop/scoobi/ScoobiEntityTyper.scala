package edu.washington.cs.knowitall.browser.hadoop.scoobi

import com.nicta.scoobi.Scoobi._
import com.nicta.scoobi.DList._
import com.nicta.scoobi.DList
import com.nicta.scoobi.io.text.TextInput._
import com.nicta.scoobi.io.text.TextInput
import com.nicta.scoobi.io.text.TextOutput._
import com.nicta.scoobi.io.text.TextOutput

import edu.washington.cs.knowitall.browser.extraction.FreeBaseType
import edu.washington.cs.knowitall.browser.extraction.Extraction
import edu.washington.cs.knowitall.browser.extraction.ExtractionGroup
import edu.washington.cs.knowitall.browser.extraction.ReVerbExtractionGroup
import edu.washington.cs.knowitall.browser.hadoop.util.FbTypeLookup

import edu.washington.cs.knowitall.common.Timing

/**
  * Does type lookup for freebase entities (fills in the argXTypes field in an extractionGroup)
  */
object ScoobiEntityTyper {
  
  val minCompletionSeconds = 90
  
  import ScoobiEntityLinker.getRandomElement
  
  lazy val fbEntityIndexes = ScoobiEntityLinker.getScratch("browser-freebase/type-lookup-index/")
  lazy val fbTypeEnumFile = "/scratch/browser-freebase/fbTypeEnum.txt"

  private lazy val fbLookupTables = fbEntityIndexes.map(index=>new FbTypeLookup(index, fbTypeEnumFile))

  def typeSingleGroup[E <: Extraction](group: ExtractionGroup[E]): ExtractionGroup[E] = {

    val arg1Types = group.arg1Entity match {
      case Some(entity) => getRandomElement(fbLookupTables).getTypesForEntity(entity.fbid).flatMap(FreeBaseType.parse(_))
      case None => Nil
    }
    val arg2Types = group.arg2Entity match {
      case Some(entity) => getRandomElement(fbLookupTables).getTypesForEntity(entity.fbid).flatMap(FreeBaseType.parse(_))
      case None => Nil
    }

    new ExtractionGroup[E](
      group.arg1Norm,
      group.relNorm,
      group.arg2Norm,
      group.arg1Entity,
      group.arg2Entity,
      arg1Types.toSet,
      arg2Types.toSet,
      group.instances)
  }

  def typeGroups(groupStrings: DList[String]): DList[String] = {

    var groupsProcessed = 0
    
    groupStrings.flatMap { groupString =>
      groupsProcessed += 1
      if (groupsProcessed % 100000 == 0) System.err.println("Groups processed: %s".format(groupsProcessed))
      val groupOption = ReVerbExtractionGroup.fromTabDelimited(groupString.split("\t"))._1
      groupOption match {
        case Some(group) => {
          val typedGroup = typeSingleGroup(group)
          Some(ReVerbExtractionGroup.toTabDelimited(typedGroup))
        }
        case None => None
      }
    }
  }

  def main(args: Array[String]) = withHadoopArgs(args) { remainingArgs =>

    conf.set("mapred.job.name", "browser entity linker")

    val (inputPath, outputPath) = (remainingArgs(0), remainingArgs(1))

    val lines: DList[String] = TextInput.fromTextFile(inputPath)

    val (completionTime, typedGroups): (Long, DList[String]) = Timing.time(typeGroups(lines))

    val remainingSeconds = minCompletionSeconds - completionTime / Timing.Seconds.divisor
    
    Thread.sleep(remainingSeconds * 1000)
    
    DList.persist(TextOutput.toTextFile(typedGroups, outputPath + "/"));
  }
}