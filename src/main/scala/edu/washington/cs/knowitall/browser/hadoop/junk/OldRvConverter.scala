package edu.washington.cs.knowitall.browser.hadoop.junk

import scopt.OptionParser
import scala.io.Source
import scala.collection.JavaConversions._

import edu.washington.cs.knowitall.commonlib.Range
import edu.washington.cs.knowitall.nlp.OpenNlpChunkedSentenceParser
import edu.washington.cs.knowitall.nlp.ChunkedSentence

import edu.washington.cs.knowitall.browser.extraction.ReVerbExtraction

import edu.washington.cs.knowitall.common.Timing._
import edu.washington.cs.knowitall.collection.immutable.Interval

import java.io.PrintStream

object OldRvConverter {

  val chunkedNlpParser = new OpenNlpChunkedSentenceParser

  object settings {

    var arg1Col: Int = -1
    var relCol: Int = -1
    var arg2Col: Int = -1
    var sentCol: Int = -1
    var urlDomCol: Int = -1
    var urlPathCol: Int = -1
    var minSecs: Int = -1

    var rangeLength = false

    def maxCol = Seq(arg1Col, relCol, arg2Col, sentCol).max
  }

  def main(args: Array[String]): Unit = {

    val parser = new OptionParser() {

      arg("arg1", "Column of arg1 range", { str => settings.arg1Col = str.toInt })
      arg("rel", "Column of rel range", { str => settings.relCol = str.toInt })
      arg("arg2", "Column of arg2 range", { str => settings.arg2Col = str.toInt })
      arg("sent", "Column of OpenNLP formatted chunked Sentence", { str => settings.sentCol = str.toInt })
      arg("urldom", "Column of Source URL domain", { str => settings.urlDomCol = str.toInt })
      arg("urlsrc", "Column of Source URL path", { str => settings.urlPathCol = str.toInt })

      opt("m", "minTime", "wait at least _ seconds before exiting", { str => settings.minSecs = str.toInt })
      opt("r", "rangeLength", "process ranges as [start, length] rather than [start, end]", { settings.rangeLength = true })
    }

    if (parser.parse(args)) {
      run
    }
  }

  def run: Unit = {

    val timeToRun = time {
      val input = Source.fromInputStream(System.in)

      val output = System.out

      input.getLines.flatMap(convertLine(_)).foreach(output.println(_))

      output.flush
    }
    
   val secs = timeToRun / Seconds.divisor
   val wait = settings.minSecs - secs
   
   if (wait > 0) Thread.sleep(wait)
   
  }

  private val splitPattern = "\t".r

  def convertLine(line: String): Option[String] = {

    def lineFailure = { System.err.println("Failure converting line: " + line); None }

    val split = splitPattern.split(line)

    if (split.length < settings.maxCol) return lineFailure

    def ranges = Seq(settings.arg1Col, settings.relCol, settings.arg2Col).map(split(_)).flatMap(rangeFromString(_))

    val sentence = tryParsingSentence(split(settings.sentCol))
    
    val sourceUrl = split(settings.urlDomCol)+split(settings.urlPathCol)

    if (!sentence.isDefined) return lineFailure

    ranges match {
      case Seq(arg1Range, relRange, arg2Range) => {
        val extr = buildExtraction(arg1Range, relRange, arg2Range, sentence.get, sourceUrl)
        extr match {
          case Some(e) => Some(ReVerbExtraction.toTabDelimited(e))
          case None => lineFailure
        }
      }
      case _ => lineFailure
    }
  }

  private val numExtractorPattern = "([0-9]+)".r

  private def rangeFromString(str: String): Option[Range] = {

    val matches = for (s <- numExtractorPattern.findAllIn(str)) yield s

    matches toSeq match {
      case Seq(num1, num2) => {
        val start = Integer.parseInt(num1)
        val lengt =
          if (!settings.rangeLength) Integer.parseInt(num2) - start
          else Integer.parseInt(num2)
        Some(new Range(start, lengt))
      }
      case _ => { System.err.println("Couldn't parse range:" + str); None }
    }
  }
  
  private def tryParsingSentence(str: String): Option[ChunkedSentence] = {
    try {
      Some(chunkedNlpParser.parseSentence(str))
    } catch {
      case e: Exception => { e.printStackTrace(); None }
    }
  }
  
  implicit def rangeToInterval(range: Range): Interval = Interval.closed(range.getStart, range.getLastIndex)

  private def buildExtraction(arg1Range: Range, relRange: Range, arg2Range: Range, sentence: ChunkedSentence, sourceUrl: String): Option[ReVerbExtraction] = {
   
    
   
    try {
      // verification
      sentence.getTokens(arg1Range)
      sentence.getTokens(relRange)
      sentence.getTokens(arg2Range)
      val sentenceTokens =  ReVerbExtraction.chunkedTokensFromLayers(sentence.getTokens.toIndexedSeq, sentence.getPosTags.toIndexedSeq, sentence.getChunkTags.toIndexedSeq)
      
      Some(new ReVerbExtraction(sentenceTokens.toIndexedSeq, arg1Range, relRange, arg2Range, sourceUrl))
      
    } catch {
      case e: Exception => { e.printStackTrace(); None }
    }
    
  }
}
