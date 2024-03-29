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

import edu.washington.cs.knowitall.nlp.extraction.ChunkedExtraction

object ScoobiGroupToAnswer extends ScoobiApp {
  def run() = {
    val (inputPath, outputPath) = (args(0), args(1))

    // serialized groups
    val groups: DList[String] = fromTextFile(inputPath)

    // serialized ExtractionGroup[ReVerbExtraction]
    val filtered: DList[String] = groups.flatMap { pickle =>
      val group = ReVerbExtractionGroup.deserializeFromString(pickle)

      // combinations
      group.map(g => g.copy(arg1 = g.arg1.copy(norm = "")))
      group.map(g => g.copy(rel = g.rel.copy(norm = "")))
      group.map(g => g.copy(arg2 = g.arg2.copy(norm = "")))

      group.map(g => g.copy(arg1=g.arg1.copy(norm = ""), rel=g.rel.copy(norm = "")))
      group.map(g => g.copy(arg1=g.arg1.copy(norm = ""), arg2=g.arg2.copy(norm = "")))
      group.map(g => g.copy(rel=g.rel.copy(norm = ""), arg2=g.arg2.copy(norm = "")))

      group.map(ReVerbExtractionGroup.serializeToString(_))
    }

    persist(TextOutput.toTextFile(filtered, outputPath + "/"));
  }
}
