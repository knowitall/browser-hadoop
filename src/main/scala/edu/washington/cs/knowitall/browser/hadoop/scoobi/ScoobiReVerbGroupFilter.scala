/*
package edu.washington.cs.knowitall.browser.hadoop.scoobi

import com.nicta.scoobi.Scoobi._

import java.io.File
import java.io.FileWriter

import scala.util.Random
import scala.collection.JavaConversions._
import scala.collection.mutable

import edu.washington.cs.knowitall.common.Timing._
import edu.washington.cs.knowitall.browser.extraction.ReVerbExtraction
import edu.washington.cs.knowitall.browser.extraction.ExtractionGroup
import edu.washington.cs.knowitall.browser.extraction.ReVerbExtractionGroup
import edu.washington.cs.knowitall.browser.entity.EntityLinker
import edu.washington.cs.knowitall.browser.entity.Pair

import edu.washington.cs.knowitall.browser.lucene.bad.BadQuery

import edu.washington.cs.knowitall.nlp.extraction.ChunkedExtraction

object ScoobiReVerbGroupFilter extends ScoobiApp {
  
  def run() = {

    val (inputPath, outputPath) = (args(0), args(1))

    // serialized groups
    val groups: DList[String] = fromTextFile(inputPath)
    
    // serialized ExtractionGroup[ReVerbExtraction]
    val filtered: DList[String] = groups.flatMap  { group => 
      val parsed = ReVerbExtractionGroup.deserializeFromString(group)
      val apply = BadQuery.applyFilterForIndex(parsed) map ReVerbExtractionGroup.serializeToString
      apply
    }
    
    persist(TextOutput.toTextFile(filtered, outputPath + "/"));
  }
}
*/
