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

/**
  * Does type lookup for freebase entities (fills in the argXTypes field in an extractionGroup)
  */
object ScoobiEntityTyper {

  val fbEntityToEnumFile = "/scratch/browser-freebase/fbEntityToEnum.txt"
  val fbTypeEnumFile = "/scratch/browser-freebase/fbTypeEnum.txt"

  val fbLookupLocal = new ThreadLocal[FbTypeLookup]() { override def initialValue() = new FbTypeLookup(fbEntityToEnumFile, fbTypeEnumFile) }

  def typeSingleGroup[E <: Extraction](group: ExtractionGroup[E]): ExtractionGroup[E] = {

    val fbLookupTable = fbLookupLocal.get

    val arg1Types = group.arg1Entity match {
      case Some(entity) => fbLookupTable.getTypesForEntity(entity.fbid).map(FreeBaseType(_))
      case None => Nil
    }
    val arg2Types = group.arg2Entity match {
      case Some(entity) => fbLookupTable.getTypesForEntity(entity.fbid).map(FreeBaseType(_))
      case None => Nil
    }

    new ExtractionGroup[E](
      group.arg1Norm,
      group.relNorm,
      group.arg2Norm,
      group.arg1Entity,
      group.arg2Entity,
      arg1Types,
      arg2Types,
      group.instances)
  }

  def typeGroups(groupStrings: DList[String]): DList[String] = {

    groupStrings.flatMap { groupString =>
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

    val typedGroups: DList[String] = typeGroups(lines)

    DList.persist(TextOutput.toTextFile(typedGroups, outputPath + "/"));
  }
}