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
import edu.washington.cs.knowitall.browser.extraction.ExtractionGroup
import edu.washington.cs.knowitall.browser.extraction.ReVerbExtractionGroup
import edu.washington.cs.knowitall.browser.entity.TopCandidatesFinder
import edu.washington.cs.knowitall.browser.entity.EntityLinker
import edu.washington.cs.knowitall.browser.entity.Pair

import edu.washington.cs.knowitall.browser.lucene.bad.BadQuery

import edu.washington.cs.knowitall.nlp.extraction.ChunkedExtraction

object ScoobiReVerbGroupFilter {
  
  def main(args: Array[String]) = withHadoopArgs(args) { a =>

    val (inputPath, outputPath) = (a(0), a(1))

    // serialized groups
    val groups: DList[String] = TextInput.fromTextFile(inputPath)
    
    // serialized ExtractionGroup[ReVerbExtraction]
    val filtered: DList[String] = groups.flatMap  { group => 
      val parsed = ReVerbExtractionGroup.fromTabDelimited(group.split("\t"))._1
      val apply = BadQuery.applyFilterForIndex(parsed) map ReVerbExtractionGroup.toTabDelimited
      apply
    }
    
    DList.persist(TextOutput.toTextFile(filtered, outputPath + "/"));
  }
}
