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

object ScoobiSentenceChunker {
  
  lazy val chunker = new OpenNlpChunker
  
  
  def main(args: Array[String]): Unit = withHadoopArgs(args) { a =>

    var inputPath, outputPath = ""

    val parser = new OptionParser() {
      arg("inputPath", "hdfs input path, sentences each on a line", { str => inputPath = str })
      arg("outputPath", "hdfs output path, chunked sentences", { str => outputPath = str })
    }

    if (!parser.parse(a)) return
    
    // serialized ReVerbExtractions
    val lines: DList[String] = TextInput.fromTextFile(inputPath)

    def sentenceToTriple(toks: Seq[ChunkedToken]): String = {
      val strs = toks.map(_.string.trim).mkString(" ")
      val poss = toks.map(_.postag.trim).mkString(" ")
      val chks = toks.map(_.chunk.trim).mkString(" ")
      
      Seq(strs, poss, chks).mkString("\t")
    }
    
    val output = lines.map { line =>
      val toks = chunker.chunk(line)
      val result = sentenceToTriple(toks)
      result
    }
    
    DList.persist(TextOutput.toTextFile(output, outputPath + "/"));
  } 
}