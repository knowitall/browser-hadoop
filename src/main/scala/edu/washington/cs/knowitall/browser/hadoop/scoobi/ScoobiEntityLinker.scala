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

import edu.washington.cs.knowitall.nlp.extraction.ChunkedExtraction

/**
 * A mapper + reducer job that
 * takes tab-delimited ReVerbExtractions as input, groups them by a normalization key, and
 * then constructs ExtractionGroup[ReVerbExtraction] from the reducer input. The Entity Linker
 * code is run in the reducer.
 */
class ScoobiEntityLinker(val stemmer: TaggedStemmer) {

  case class RVTuple(arg1: String, rel: String, arg2: String) {
    override def toString = "%s__%s__%s".format(arg1, rel, arg2)
  }
  


  def getEntity(el: EntityLinker, arg: String, head: ReVerbExtraction, sources: Seq[String]): Option[ArgEntity] = {

       
    var argEntity: Option[ArgEntity] = None

    val tryEL = el.getBestFbidFromSources(arg, sources)
    if (tryEL != null) argEntity = Some(ArgEntity(tryEL.one, tryEL.two))
    argEntity
  }

  def linkEntities(el: EntityLinker, group: ExtractionGroup[ReVerbExtraction]): ExtractionGroup[ReVerbExtraction] = {

    val extrs = group.instances.map(_._1)

    val head = extrs.head

    val sources = extrs.map(e => e.source.getSentence().getTokensAsString()).toSeq

    val arg1Entity = if (head.source.getArgument1().getPosTags().exists(_.startsWith("NNP"))) {
      getEntity(el, head.arg1Tokens, head, sources)
    } else {
      None
    }
    
    val arg2Entity = if (head.source.getArgument2().getPosTags().exists(_.startsWith("NNP"))) {
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
      group.instances.map(tup=>(tup._1, "g1b", tup._3)))

    newGroup
  }

}

object ScoobiEntityLinker {

  var random = new scala.util.Random
  
  // hardcoded for the rv cluster...
  val baseIndex = /*/scratch*/"browser-freebase/3-context-sim/index"
  
  
    
  def getScratch: String = {
    
    
    try {
      val num = random.nextInt % 4
      if (num == 0) return "/scratch/"
      else return "/scratch%d/".format(num)
    } catch {
      case e: NumberFormatException => { 
        e.printStackTrace()
        System.err.println("Error getting index")
        return "/scratch/"
        }
    }
  }
    
  val linkerCache = new mutable.HashMap[Thread, EntityLinker]
  
  def main(args: Array[String]) = withHadoopArgs(args) { a =>

    val flink = new ScoobiEntityLinker(TaggedStemmer.getInstance)

    val (inputPath, outputPath) = (a(0), a(1))

    val lines: DList[String] = TextInput.fromTextFile(inputPath)

    val linkedGroups: DList[String] = lines.flatMap { line => 
      val el = linkerCache.getOrElseUpdate(Thread.currentThread(), new EntityLinker(getScratch+ baseIndex))
    	val extrOp = ReVerbExtractionGroup.fromTabDelimited(line.split("\t"))._1
    	extrOp match {
        case Some(extr) => Some(ReVerbExtractionGroup.toTabDelimited(flink.linkEntities(el, extr)))
        case None => None
      }
    }

    DList.persist(TextOutput.toTextFile(linkedGroups, outputPath + "/"));
  }
}
