package edu.washington.cs.knowitall.browser.hadoop.util

import edu.washington.cs.knowitall.browser.extraction.ReVerbExtractionGroup
import edu.washington.cs.knowitall.browser.extraction.ExtractionGroup
import edu.washington.cs.knowitall.browser.extraction.ReVerbExtraction

import scala.io.Source

object GroupPrettyPrinter {

  lazy val tabHeaders = Seq("arg1", "rel", "arg2", "arg1_entity", "arg2_entity", "sentences...").mkString("\t")
  
  def getPrettyGroup(group: ExtractionGroup[ReVerbExtraction]): String = {
    
    val head = group.instances.head.extraction
    val sources = group.instances.map(_.extraction.sentenceTokens).map(sent=>sent.map(_.string).mkString(" "))
    
    val arg1 = head.getTokens(head.arg1Interval).mkString(" ")
    val rel = head.getTokens(head.relInterval).mkString(" ")
    val arg2 = head.getTokens(head.arg2Interval).mkString(" ")
    
    val arg1E = group.arg1Entity match {
      case Some(entity) => (entity.name, entity.fbid).toString
      case None => "none"
    }
    val arg2E = group.arg2Entity match {
      case Some(entity) => (entity.name, entity.fbid).toString
      case None => "none"
    }
    
    val relFields = Seq(arg1, rel, arg2, arg1E, arg2E)
    val allOutput = relFields ++ sources
    
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