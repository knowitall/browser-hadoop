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

import edu.washington.cs.knowitall.tool.chunk.OpenNlpChunker
import edu.washington.cs.knowitall.tool.chunk.ChunkedToken

import scopt.OptionParser

import scala.collection.mutable

object UnlinkableEntityTyper {
  
  type StringREG = String
  type REG = ExtractionGroup[ReVerbExtraction]
  
  private val tabSplit = "\t".r
  
  sealed abstract class ArgField
  case class Arg1 extends ArgField
  case class Arg2 extends ArgField
  
  case class MutableInt(var value: Int) {
    def increment = value += 1
  }
  
  def lineToOptGroup(line: StringREG): Option[REG] = ReVerbExtractionGroup.fromTabDelimited(tabSplit.split(line))._1
  
  def mp1Pair(group: REG): (String, StringREG) = (group.rel.norm, ReVerbExtractionGroup.toTabDelimited(group))
  
  // build a map of entity => count while making only a single pass over the data
  def reducer1Helper(stringRegIter: Iterator[StringREG], argField: ArgField): Map[String, MutableInt] = {
    
    def getEntity(reg: REG) = argField match {
      case Arg1() => reg.arg1.entity
      case Arg2() => reg.arg2.entity
    }
    
    def getFbid(fbEnt: FreeBaseEntity) = fbEnt.fbid
    
    val entityIter = stringRegIter flatMap lineToOptGroup flatMap getEntity map getFbid
    
    // here is where we spend the iterator:
    val entityCounts = new mutable.HashMap[String, MutableInt]
    entityIter.foreach { fbid =>
      entityCounts.getOrElseUpdate(fbid, MutableInt(0)).increment
    }
    entityCounts toMap
  }
  
  def typesForEntity(fbid: String): Iterable[String] = TyperResources.typeLookup(fbid)
  
  def relationWeight(entityCounts: Map[String, MutableInt]): Double = -1.0 // NOT IMPLEMENTED YET
  
  def reducer1Process(input: (String, Iterable[StringREG]), argField: ArgField): (Iterable[StringREG], Iterable[(String, Int)], Double) = {
    
	val entityCounts = reducer1Helper(input._2.iterator, argField)
	
	// compute relation weight
	val weight = relationWeight(entityCounts)
	
	// return entity counts (or type counts?) and relation weight.
	// Hope we can return the Iterable[StringREG] without problems even though we've traversed it once.
	null
  }
  
  def main(args: Array[String]): Unit = withHadoopArgs(args) { a =>

    var inputPath, outputPath = ""
    var argField: ArgField = Arg1()

    val parser = new OptionParser() {
      arg("inputPath", "hdfs input path, ExtractionGroups", { str => inputPath = str })
      arg("outputPath", "hdfs output path, ExtractionGroups", { str => outputPath = str })
      arg("arg", "arg1 to predict types for arg1's, arg2 to predict types for arg2s", { str => 
        if (str.equals("arg1")) argField = Arg1()
        else if (str.equals("arg2")) argField = Arg2()
        else throw new IllegalArgumentException("arg must be either arg1 or arg2")
      })
    }

    if (!parser.parse(a)) return
    
    // serialized ReVerbExtractions
    val lines: DList[StringREG] = TextInput.fromTextFile(inputPath)
    val groups = lines flatMap lineToOptGroup
    
    // first, we want to group by relation in order to compute relation weight and entity range. 
    val mapper1Pairs = groups map mp1Pair

    // begin the reduce phase by calling groupByKey
    val reducer1 = mapper1Pairs.groupByKey
    
    // given each element of reducer1, we compute:
    // (Iterable[REG], entity range, relation weight)
    // type lookup must occur at this point in order to compute the relation weight.
  } 
}

private object TyperResources {
  
  import scala.util.Random
  import edu.washington.cs.knowitall.browser.entity.EntityTyper
  
  private val numScratchDisks = 4
  
  private def loadTypers: Seq[EntityTyper] = {
    ScoobiEntityLinker.getScratch(numScratchDisks)(ScoobiEntityLinker.baseIndex).map(base=>new EntityTyper(base))
  }
  
  def typeLookup(fbid: String): Iterable[String] = {
    val typers = typeLookupLocal.get
    val randTyper = typers(Random.nextInt(numScratchDisks))
    val types = randTyper.typeFbid(fbid)
    types
  }
  
  private val typeLookupLocal = new ThreadLocal[Seq[EntityTyper]]() { override def initialValue = loadTypers }
}