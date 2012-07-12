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
  case class Arg1() extends ArgField
  case class Arg2() extends ArgField
  
  case class MutableInt(var value: Int) {
    def increment = value += 1
  }
  
  def lineToOptGroup(line: StringREG): Option[REG] = ReVerbExtractionGroup.fromTabDelimited(tabSplit.split(line))._1
  
  def mp1Pair(group: REG): (String, StringREG) = (group.rel.norm, ReVerbExtractionGroup.toTabDelimited(group))

  def typeToString(typ: FreeBaseType): String = "/%s/%s".format(typ.domain, typ.typ) 
  
  // Converts an REG into entity fbid and types.
  def loadEntityInfo(argField: ArgField)(stringReg: StringREG): Option[(String, Set[String])] = {
    val group = lineToOptGroup(stringReg).get
    
    argField match {
      case Arg1() => group.arg1.entity match {
        case Some(entity) => Some((entity.fbid, group.arg1.types map typeToString))
        case None => None
      }
      case Arg2() => group.arg2.entity match {
        case Some(entity) => Some((entity.fbid, group.arg2.types map typeToString))
        case None => None
      }
    }
  } 
  
  def allPairs(strings: IndexedSeq[String]): Iterable[(String, String)] = {
    
    val length = strings.length
    
    (0 until length).flatMap { i =>
      (i until length).map { j =>
        (strings(i), strings(j))  
      }
    }
  }
  
  def calculateRelWeight(entityFrqTypMap: Map[String,(Int, Set[String])]): Double = {
    
    
    // now we perform the summation tom describes 
    // the first map produces the terms of the sum
    val terms = allPairs(entityFrqTypMap.keySet.toIndexedSeq) map { case (fbid1, fbid2) =>
      
      val types1 = entityFrqTypMap(fbid1)._2
      val types2 = entityFrqTypMap(fbid2)._2
      
      // do types1 and types2 intersect?
      if (types1.intersect(types2).isEmpty) 0.0 else 1.0
    }
    
    // we sum the terms and then apply tom's denominator 
    val domainSize = entityFrqTypMap.size.toDouble
    val denominator = (domainSize * (domainSize - 1))
    terms.sum / denominator
  }
  
  // produces List of Type, Frequency of that type as well as relation weight, as a double.
  def reducer1Process(argField: ArgField)(groups: Iterable[StringREG]): (List[(String, Int)], Double) = {
    
    val entitiesWithTypes = groups flatMap loadEntityInfo(argField)
    // group by fbid and make counts
    
    // map from fbid to (count, typeStrings)
    val entitiesGrouped = entitiesWithTypes.groupBy(_._1) map { case (fbid, entityInfos) =>
      (fbid, (entityInfos.size, entityInfos.head._2))  
    } toMap
    
    // compute relation weight by considering all pairs of entities
    val relWeight = calculateRelWeight(entitiesGrouped)
    
    // now we just need to make a properly counted list of types to pass to the next mapper.
    // this means expanding entitiesGrouped to type,freq pairs, and then grouping by type 
    // and summing to get a map from type->frequency of that type.
    def toTypeFreqPairs(value: (Int, Set[String])): Iterable[(String, Int)] = value._2.map((_, value._1))
    def typeAccumulator(entry: (String, Iterable[(String, Int)])): (String, Int) = (entry._1, entry._2.map(_._2).sum)
    
    val typeFreqMap = entitiesGrouped.values flatMap toTypeFreqPairs groupBy(_._1) map typeAccumulator
    
    (typeFreqMap.toList, relWeight)
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
    val reducer1Output = reducer1.map { case (rel, groups) =>
      val (typeFreqs, relWeight) = reducer1Process(argField)(groups)
      (rel, typeFreqs.mkString(","), groups.mkString("_GDL_"), relWeight).toString
    }
    
    DList.persist(TextOutput.toTextFile(reducer1Output, outputPath + "/"));
  } 
}