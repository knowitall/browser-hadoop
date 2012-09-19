package edu.washington.cs.knowitall.browser.hadoop.scoobi

import scopt.OptionParser
import util.ExtractionSentenceRecord

import com.nicta.scoobi.Scoobi._
import edu.washington.cs.knowitall.tool.stem.MorphaStemmer
import edu.washington.cs.knowitall.tool.postag.PostaggedToken

import RelationCounter.stemmer
import RelationCounter.filterTokens

object RelTupleTabulator extends ScoobiApp {

  case class ArgContext(val arg1: String, val arg2: String)
  
  def run(): Unit = {

    var inputPath = ""
    var outputPath = ""
    var minFrequency = 0
    var maxFrequency = Integer.MAX_VALUE

    val parser = new OptionParser() {
      arg("inputPath", "file input path, records delimited by newlines", { str => inputPath = str })
      arg("outputPath", "file output path, newlines again", { str => outputPath = str })
      intOpt("minFreq", "don't keep tuples below this frequency", { num => minFrequency = num })
      intOpt("maxFreq", "don't keep tuples above this frequency", { num => maxFrequency = num })
    }

    if (!parser.parse(args)) return
 
    println("Parsed args: %s".format(args.mkString(" ")))
    
    val input: DList[String] = fromTextFile(inputPath)
    
    val tuples = input flatMap toTuple
    
    val grouped = tuples.groupByKey
    
    val freqFilteredRels = grouped.flatMap { case (rel, argContexts) =>
      val size = argContexts.size
      if (size >= minFrequency && size <= maxFrequency) Some((rel, "")) else None
    }
    
    val filteredRelContexts = freqFilteredRels.join(grouped)
    
    val outputStrings = filteredRelContexts.flatMap { case (rel, (empties, contexts)) =>
      contexts.take(100000).map { context => 
        Seq(rel, context.arg1, context.arg2).mkString("\t")
      }  
    }
    
    persist(toTextFile(outputStrings, outputPath + "/"))
  }
  
  
  // (rel tokens, (rel.toString, arg1String, arg2String))
  def toTuple(inputRecord: String): Option[(String, ArgContext)] = {
    try { 
      val esr = new ExtractionSentenceRecord(inputRecord)
      val relTokens = esr.norm1Rel.split(" ").map(_.toLowerCase)
      val relPos = esr.norm1RelPosTags.split(" ")
      val posTokens = relTokens.zip(relPos) map { case (tok, pos) => new PostaggedToken(pos, tok, 0) } filter filterTokens
      val stemTokens = posTokens map stemmer.stemToken map { lemma => new PostaggedToken(lemma.token.postag, lemma.lemma, 0) }
      val relString = stemTokens.map(_.string).mkString(" ")
      if (posTokens.isEmpty) None 
      else Some(relString, ArgContext(esr.norm1Arg1.toLowerCase, esr.norm1Arg2.toLowerCase))
    } 
    catch { case e: Exception => { e.printStackTrace; None }}
  }
}