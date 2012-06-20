package edu.washington.cs.knowitall.browser.hadoop.scoobi

import com.nicta.scoobi.Scoobi._
import com.nicta.scoobi.DList._
import com.nicta.scoobi.DList
import com.nicta.scoobi.io.text.TextInput._
import com.nicta.scoobi.io.text.TextInput
import com.nicta.scoobi.io.text.TextOutput._
import com.nicta.scoobi.io.text.TextOutput

import edu.washington.cs.knowitall.browser.entity.Entity

import edu.washington.cs.knowitall.browser.extraction.FreeBaseType
import edu.washington.cs.knowitall.browser.extraction.Extraction
import edu.washington.cs.knowitall.browser.extraction.ExtractionGroup
import edu.washington.cs.knowitall.browser.extraction.ReVerbExtractionGroup
import edu.washington.cs.knowitall.browser.hadoop.util.FbTypeLookup

import edu.washington.cs.knowitall.common.Timing

import scala.collection.JavaConversions._

/**
  * Does type lookup for freebase entities (fills in the argXTypes field in an extractionGroup)
  */
class ScoobiEntityTyper {

  def getRandomElement[T](seq: Seq[T]): T = seq(scala.util.Random.nextInt(seq.size))

  lazy val fbEntityIndexes = ScoobiEntityLinker.getScratch("browser-freebase/type-lookup-index/")
  lazy val fbTypeEnumFile = "/scratch/browser-freebase/fbTypeEnum.txt"

  private lazy val fbLookupTables = fbEntityIndexes.map(index => new FbTypeLookup(index, fbTypeEnumFile))

  /**
   * mutator method to 
   */
  def typeEntity(entity: Entity): Entity = {
    
    val fbid = entity.fbid

    val types = getRandomElement(fbLookupTables).getTypesForEntity(entity.fbid)
    
    entity.attachTypes(types)

    return entity
  }
  
  private def typeSingleGroup[E <: Extraction](group: ExtractionGroup[E]): ExtractionGroup[E] = {

    val arg1Types = group.arg1.entity match {
      case Some(entity) => getRandomElement(fbLookupTables).getTypesForEntity(entity.fbid).flatMap { typeString =>
        try { FreeBaseType.parse(typeString) }
        catch { case iae: IllegalArgumentException => { System.err.println(typeString); None } }
      }
      case None => Nil
    }
    val arg2Types = group.arg2.entity match {
      case Some(entity) => getRandomElement(fbLookupTables).getTypesForEntity(entity.fbid).flatMap { typeString =>
        try { FreeBaseType.parse(typeString) }
        catch { case iae: IllegalArgumentException => { System.err.println(typeString); None } }
      }
      case None => Nil
    }

    new ExtractionGroup[E](
      group.arg1.norm,
      group.rel.norm,
      group.arg2.norm,
      group.arg1.entity,
      group.arg2.entity,
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
}
object ScoobiEntityTyper {

  val minCompletionSeconds = 90

  val typerLocal = new ThreadLocal[ScoobiEntityTyper]() { override def initialValue() = new ScoobiEntityTyper() }
  
  def main(args: Array[String]) = withHadoopArgs(args) { remainingArgs =>

    conf.set("mapred.job.name", "browser entity linker")

    val (inputPath, outputPath) = (remainingArgs(0), remainingArgs(1))

    val lines: DList[String] = TextInput.fromTextFile(inputPath)

    val (completionTime, typedGroups): (Long, DList[String]) = Timing.time(typerLocal.get.typeGroups(lines))

    val remainingSeconds = minCompletionSeconds - completionTime / Timing.Seconds.divisor

    Thread.sleep(remainingSeconds * 1000)

    DList.persist(TextOutput.toTextFile(typedGroups, outputPath + "/"));
  }
}