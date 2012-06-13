package edu.washington.cs.knowitall.browser.hadoop.scoobi

import com.nicta.scoobi.Scoobi._
import com.nicta.scoobi.DList._
import com.nicta.scoobi.DList
import com.nicta.scoobi.io.text.TextInput._
import com.nicta.scoobi.io.text.TextInput
import com.nicta.scoobi.io.text.TextOutput._
import com.nicta.scoobi.io.text.TextOutput

import edu.washington.cs.knowitall.common.Timing
import edu.washington.cs.knowitall.collection.immutable.Interval
import edu.washington.cs.knowitall.browser.extraction.ReVerbExtraction
import edu.washington.cs.knowitall.browser.extraction.FreeBaseEntity
import edu.washington.cs.knowitall.browser.extraction.FreeBaseType
import edu.washington.cs.knowitall.browser.extraction.Instance
import edu.washington.cs.knowitall.browser.extraction.ExtractionGroup
import edu.washington.cs.knowitall.browser.util.TaggedStemmer
import edu.washington.cs.knowitall.browser.extraction.ReVerbExtractionGroup

import edu.washington.cs.knowitall.browser.hadoop.junk.OldRvConverter.rangeToInterval

import edu.washington.cs.knowitall.extractor.ReVerbExtractor
import edu.washington.cs.knowitall.nlp.ChunkedSentence
import edu.washington.cs.knowitall.nlp.extraction.ChunkedBinaryExtraction

import edu.washington.cs.knowitall.tool.chunk.OpenNlpChunker
import edu.washington.cs.knowitall.tool.chunk.ChunkedToken

import scopt.OptionParser

import scala.collection.JavaConversions._

object ScoobiReVerb {

  lazy val extractor = new ReVerbExtractor

  def main(args: Array[String]): Unit = withHadoopArgs(args) { a =>

    var inputPath, outputPath = ""

    val parser = new OptionParser() {
      arg("inputPath", "hdfs input path, sentences each on a line", { str => inputPath = str })
      arg("outputPath", "hdfs output path, chunked sentences", { str => outputPath = str })
    }

    if (!parser.parse(a)) return

    // serialized ReVerbExtractions
    val lines: DList[String] = TextInput.fromTextFile(inputPath)

    def parseChunkedSentence(strPosChkString: String): Option[ChunkedSentence] = strPosChkString.split("\t") match {
      case Array(strings, postags, chunks, _*) => try {
        val (stringSplit, postagSplit, chunkSplit) = (strings.split(" "), postags.split(" "), chunks.split(" "))
        require(stringSplit.length == postagSplit.length && postagSplit.length == chunkSplit.length)
        val chunkedSentence = new ChunkedSentence(stringSplit, postagSplit, chunkSplit)
        Some(chunkedSentence)
      } catch {
        case e: Exception => {
          System.err.println("Error parsing chunked sentence:\n%s\nStack trace:".format(strPosChkString))
          e.printStackTrace
          System.err.println()
          None
        }
      }
      case _ => {
        System.err.println("Tab split length bad:n%s\n".format(strPosChkString))
        None
      }
    }

    def getChunkedExtractions(strPosChkString: String): Iterable[ChunkedBinaryExtraction] = {
      parseChunkedSentence(strPosChkString) match {
        case Some(chunkedSentence) => try {
          extractor.extract(chunkedSentence)
        } catch {
          case e: Exception => {
            System.err.println("Extractor exception for:\n%s\nStack trace:".format(strPosChkString))
            e.printStackTrace
            System.err.println()
            None
          }
        }
        case None => Iterable.empty
      }
    }

    def getBrowserExtractions(strPosChkString: String): Iterable[ReVerbExtraction] = {
      
      val chunkedExtractions = getChunkedExtractions(strPosChkString)
      val extractions = chunkedExtractions map { chunkedExtr => 
        val sent = chunkedExtr.getSentence
        val sentenceTokens = ReVerbExtraction.chunkedTokensFromLayers(sent.getTokens, sent.getPosTags, sent.getChunkTags).toIndexedSeq
        val (arg1Interval, relInterval, arg2Interval): (Interval, Interval, Interval) = (chunkedExtr.getArgument1.getRange, chunkedExtr.getRelation.getRange, chunkedExtr.getArgument2.getRange)
        
        val urlString = java.net.URLEncoder.encode(sentenceTokens.dropRight(1).map(_.string).mkString(" "), "UTF-8")
        val sourceUrl = "http://en.wikipedia.org/wiki/Special:Search?search=%s&go=Go".format(urlString)
        new ReVerbExtraction(sentenceTokens, arg1Interval, relInterval, arg2Interval, sourceUrl)
      }
      
      extractions
    }

    val finalExtractions = lines.flatMap { line =>
      val rvExtrs = getBrowserExtractions(line)
      rvExtrs map ReVerbExtraction.toTabDelimited
    }
    
    DList.persist(TextOutput.toTextFile(finalExtractions, outputPath + "/"));
  }
}