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

import scopt.OptionParser

// A trivially simple utility for sampling lines from a file in Hadoop. Useful for creating debugging/test files for quickly
// iterating in hadoop.
object ArgPairExtractor extends ScoobiApp {

  def run(): Unit = {

    var inputPath = ""
    var outputPath = ""

    val parser = new OptionParser() {
      arg("inputPath", "file input path, records delimited by newlines", { str => inputPath = str })
      arg("outputPath", "file output path, newlines again", { str => outputPath = str })
    }

    if (!parser.parse(args)) return

    println("Parsed args: %s".format(args.mkString(" ")))
    println("inputPath=%s".format(inputPath))
    println("outputPath=%s".format(outputPath))

    // serialized ReVerbExtractions
    val input: DList[String] = fromTextFile(inputPath)

    def argExtractor(line: String): Option[(String, String)] = {

      ReVerbExtractionGroup.deserializeFromString(line).map { reg =>
        (reg.rel.norm, reg.arg1.norm + "+" + reg.arg2.norm)
      }
    }

    val keyValues = input flatMap argExtractor

    val grouped = keyValues.groupByKey

    val output = grouped map { case (rel, argPairs) => (Iterable(rel) ++ argPairs.take(1000000)).mkString("\t") }

    persist(toTextFile(output, outputPath + "/"))
  }
}
