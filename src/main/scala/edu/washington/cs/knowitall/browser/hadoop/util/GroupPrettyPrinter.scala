package edu.washington.cs.knowitall.browser.hadoop.util

import edu.washington.cs.knowitall.browser.extraction.ReVerbExtractionGroup
import edu.washington.cs.knowitall.browser.extraction.ExtractionGroup
import edu.washington.cs.knowitall.browser.extraction.ReVerbExtraction

import scala.io.Source

object GroupPrettyPrinter {

  lazy val tabHeaders = Seq("arg1", "rel", "arg2", "arg1_entity", "arg2_entity", "sentences...").mkString("\t")
  
  def getPrettyGroup(group: ExtractionGroup[ReVerbExtraction]): String = {
    
    val head = group.instances.head.extraction
    val source = head.source
    
    val arg1 = source.getArgument1.getTokensAsString
    val rel = source.getRelation.getTokensAsString
    val arg2 = source.getArgument2.getTokensAsString
    
    val arg1E = group.arg1Entity.getOrElse("none")
    val arg2E = group.arg2Entity.getOrElse("none")
    
    val relFields = Seq(arg1, rel, arg2, arg1E, arg2E)
    val allOutput = relFields ++ group.instances.map(_.extraction.source.getSentence.getTokensAsString)
    
    allOutput.mkString("\t")
  }
  
  def main(args: Array[String]): Unit = {
    
    println(tabHeaders)
    
    Source.fromInputStream(System.in).getLines.flatMap({ line => ReVerbExtractionGroup.fromTabDelimited(line.split("\t"))._1}).foreach { group =>
      val pretty = getPrettyGroup(group)
      println(pretty)
    }
  }
}