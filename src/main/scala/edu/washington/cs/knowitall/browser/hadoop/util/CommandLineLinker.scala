package edu.washington.cs.knowitall.browser.hadoop.util

import scopt.OptionParser
import scala.io.Source
import java.io.PrintStream
import java.io.BufferedReader
import java.io.InputStreamReader

import edu.washington.cs.knowitall.common.Resource.using
import edu.washington.cs.knowitall.browser.extraction.ExtractionGroup
import edu.washington.cs.knowitall.browser.extraction.ReVerbExtractionGroup
import edu.washington.cs.knowitall.browser.extraction.ReVerbExtraction

import edu.washington.cs.knowitall.browser.hadoop.scoobi.ScoobiEntityLinker

/** reads REGs from standard input. Writes them back to standard output. */
object CommandLineLinker {

  private val tabSplit = "\t".r
  
  type REG = ExtractionGroup[ReVerbExtraction]
  
  def linkData(input: Source, output: PrintStream): Unit = {
    
    def toGroups(line: String): Option[REG] = {
      ReVerbExtractionGroup.fromTabDelimited(tabSplit.split(line))._1
    }
    
    val linker = ScoobiEntityLinker.getEntityLinker(1)
    // convert input lines to REGs
    val groups = input.getLines flatMap toGroups 
    val linkedGroups = groups map linker.linkEntities(reuseLinks = false)
    val strings = linkedGroups map ReVerbExtractionGroup.toTabDelimited
    strings foreach(output.println(_))
  }
  
  def main(args: Array[String]): Unit = {

    val inputFile = args(0)
    
    val inputReader = new BufferedReader(new InputStreamReader(System.in))
    
   linkData(Source.fromFile(inputFile), System.out)
   
   inputReader.close
  }
}