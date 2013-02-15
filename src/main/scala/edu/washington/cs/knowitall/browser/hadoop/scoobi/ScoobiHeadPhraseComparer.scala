package edu.washington.cs.knowitall.browser.hadoop.scoobi

import scala.Option.option2Iterable
import com.nicta.scoobi.Scoobi.DList
import com.nicta.scoobi.Scoobi.ScoobiApp
import com.nicta.scoobi.Scoobi.StringFmt
import com.nicta.scoobi.Scoobi.TextInput
import com.nicta.scoobi.Scoobi.TextOutput
import com.nicta.scoobi.Scoobi.persist
import edu.washington.cs.knowitall.browser.extraction.ReVerbExtractionGroup
import scopt.OptionParser
import edu.washington.cs.knowitall.browser.util.CrosswikisHandler
import edu.washington.cs.knowitall.browser.entity.util.HeadPhraseFinder

/**
  * A mapper job that takes tab-delimited ReVerbExtractions as input, and outputs the mapping
  * between the tuple args and the tuple args' head phrase.
  */
object ScoobiHeadPhraseComparer extends ScoobiApp {
  val cwHandler = new CrosswikisHandler("/scratch2/")
  
  def getHeadWords(groups:DList[String]): DList[String] = {
    groups.flatMap { line =>
      ReVerbExtractionGroup.deserializeFromString(line) match {
        case Some(extractionGroup) => {
          val firstExtraction = extractionGroup.instances.head.extraction
          val arg1String = firstExtraction.arg1Tokens.map(_.string).mkString(" ")
          val arg2String = firstExtraction.arg2Tokens.map(_.string).mkString(" ")
          val arg1Head = HeadPhraseFinder.getHeadPhrase(firstExtraction.arg1Tokens, cwHandler)
          val arg2Head = HeadPhraseFinder.getHeadPhrase(firstExtraction.arg2Tokens, cwHandler)
          val arg1Head2 = HeadPhraseFinder.getHeadPhrase2(firstExtraction.arg1Tokens)
          val arg2Head2 = HeadPhraseFinder.getHeadPhrase2(firstExtraction.arg2Tokens)
          List(
            "%s\t%s\t%s".format(arg1String, arg1Head2, arg1Head),
            "%s\t%s\t%s".format(arg2String, arg2Head2, arg2Head)
          )
        }
        case None => {
          System.err.println("Error parsing a group: %s".format(line));
          None
        }
      }
    }
  }

  def run() = {
    var inputPath, outputPath = ""
      
    val parser = new OptionParser() {
      arg("inputPath", "hdfs input path, tab delimited ExtractionGroups", { str => inputPath = str })
      arg("outputPath", "hdfs output path, tab delimited ExtractionGroups", { str => outputPath = str })
    }

    if (parser.parse(args)) {
      val lines: DList[String] = TextInput.fromTextFile(inputPath)
      val headWords: DList[String] = getHeadWords(lines)
      persist(TextOutput.toTextFile(headWords, outputPath + "/"));
    }
  }
}
