package edu.washington.cs.knowitall.browser.hadoop.scoobi

import com.nicta.scoobi.Scoobi._

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

object ScoobiReVerb extends ScoobiApp {

  private val tabSplit = "\t".r
  private val wsSplit = "\\s".r

  lazy val extractor = new ReVerbExtractor

  def run(): Unit = {

    var inputPath, outputPath = ""

    val parser = new OptionParser() {
      arg("inputPath", "hdfs input path, sentences each on a line", { str => inputPath = str })
      arg("outputPath", "hdfs output path, chunked sentences", { str => outputPath = str })
    }

    if (!parser.parse(args)) return

    // serialized ReVerbExtractions
    val lines: DList[String] = TextInput.fromTextFile(inputPath)

    def parseChunkedSentence(strs: Seq[String], poss: Seq[String], chks: Seq[String]): Option[ChunkedSentence] = {
      try {
        require(strs.length == poss.length && poss.length == chks.length)
        val chunkedSentence = new ChunkedSentence(strs, poss, chks)
        Some(chunkedSentence)
      } catch {
        case e: Exception => {
          System.err.println("Error parsing chunked sentence:\n%s\nStack trace:".format((strs, poss, chks)))
          e.printStackTrace
          System.err.println()
          None
        }
      }
    }

    def getChunkedExtractions(strs: Seq[String], poss: Seq[String], chks: Seq[String]): Iterable[ChunkedBinaryExtraction] = {
      parseChunkedSentence(strs, poss, chks) match {
        case Some(chunkedSentence) => try {
          extractor.extract(chunkedSentence)
        } catch {
          case e: Exception => {
            System.err.println("Extractor exception for:\n%s\nStack trace:".format((strs, poss, chks)))
            e.printStackTrace
            System.err.println()
            None
          }
        }
        case None => Iterable.empty
      }
    }

    def getBrowserExtractions(strs: Seq[String], poss: Seq[String], chks: Seq[String], url: String): Iterable[ReVerbExtraction] = {

      val chunkedExtractions = getChunkedExtractions(strs, poss, chks)
      val extractions = chunkedExtractions map { chunkedExtr =>
        val sent = chunkedExtr.getSentence
        val sentenceTokens = ReVerbExtraction.chunkedTokensFromLayers(sent.getTokens, sent.getPosTags, sent.getChunkTags).toIndexedSeq
        val (arg1Range, relRange, arg2Range) = (chunkedExtr.getArgument1.getRange, chunkedExtr.getRelation.getRange, chunkedExtr.getArgument2.getRange)
        val (arg1Interval, relInterval, arg2Interval) = (rangeToInterval(arg1Range), rangeToInterval(relRange), rangeToInterval(arg2Range))
        val urlString = java.net.URLEncoder.encode(sentenceTokens.dropRight(1).map(_.string).mkString(" "), "UTF-8")
        val sourceUrl = url
        new ReVerbExtraction(sentenceTokens, arg1Interval, relInterval, arg2Interval, sourceUrl)
      }

      extractions
    }

    def split(str: String) = wsSplit.split(str)

    val finalExtractions = lines.flatMap { line =>
      tabSplit.split(line) match {
        case Array(strs, poss, chks, url, _*) => {
          val rvExtrs = getBrowserExtractions(split(strs), split(poss), split(chks), url)
          rvExtrs map ReVerbExtraction.serializeToString
        }
        case _ => {
          System.err.println("Couldn't parse line: %s".format(line))
          Seq.empty
        }
      }
    }

    persist(TextOutput.toTextFile(finalExtractions, outputPath + "/"));
  }
}