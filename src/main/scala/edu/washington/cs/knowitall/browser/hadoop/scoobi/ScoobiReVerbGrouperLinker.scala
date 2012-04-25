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

object ScoobiReVerbGrouperLinker {
  
  def main(args: Array[String]) = withHadoopArgs(args) { a =>

    val (inputPath, outputPath, corpus) = (a(0), a(1), a(2))

    // serialized ReVerbExtractions
    val extrs: DList[String] = TextInput.fromTextFile(inputPath)
    
    // serialized ExtractionGroup[ReVerbExtraction]
    val groups = ScoobiReVerbGrouper.groupExtractions(extrs, corpus)
    
    val linkedGroups = ScoobiEntityLinker.linkGroups(groups)
    
    DList.persist(TextOutput.toTextFile(linkedGroups, outputPath + "/"));
  }
}
