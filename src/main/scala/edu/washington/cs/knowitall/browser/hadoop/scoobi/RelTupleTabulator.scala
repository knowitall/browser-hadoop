package edu.washington.cs.knowitall.browser.hadoop.scoobi

import scopt.OptionParser
import util.ExtractionSentenceRecord

import com.nicta.scoobi.Scoobi._
import edu.washington.cs.knowitall.tool.stem.MorphaStemmer
import edu.washington.cs.knowitall.tool.postag.PostaggedToken

import RelationCounter.stemmer
import RelationCounter.filterTokens

object RelTupleTabulator extends ScoobiApp {

  case class ArgContext(val arg1: Seq[PostaggedToken], val arg2: Seq[PostaggedToken]) {
    override def toString = {
      val arg1Tokens = arg1.map(_.string).mkString(" ")
      val arg1Postags = arg1.map(_.postag).mkString(" ")
      val arg2Tokens = arg2.map(_.string).mkString(" ")
      val arg2Postags = arg2.map(_.postag).mkString(" ")
      Seq(arg1Tokens, arg1Postags, arg2Tokens, arg2Postags).mkString("\t")
    }
  }
  
  object ArgContext {
    def fromString(str: String): Option[ArgContext] = {
      str.split("\t") match {
        case Array(arg1Tokens, arg1Postags, arg2Tokens, arg2Postags, _*) => {
          val arg1 = joinTokensAndPostags(arg1Tokens, arg1Postags)
          val arg2 = joinTokensAndPostags(arg2Tokens, arg2Postags)
          Some(ArgContext(arg1, arg2))
        }
        case _ => None
      }
    }
  }
  
    def joinTokensAndPostags(tokens: String, postags: String): Seq[PostaggedToken] = {
    tokens.split(" ").zip(postags.split(" ")).map { case (tok, pos) =>
      new PostaggedToken(pos.toLowerCase, tok.toLowerCase, 0)
    }
  }
  
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
        Seq(rel, context.toString).mkString("\t")
      }  
    }
    
    persist(toTextFile(outputStrings, outputPath + "/"))
  }
  
  def stemToken(token: PostaggedToken): PostaggedToken = {
    val lemma = stemmer.stemToken(token)
    new PostaggedToken(lemma.token.postag, lemma.lemma, 0)
  }
  
  // (rel tokens, (rel.toString, arg1String, arg2String))
  def toTuple(inputRecord: String): Option[(String, String)] = {
    try { 
      val esr = new ExtractionSentenceRecord(inputRecord)
      val relTokens = joinTokensAndPostags(esr.norm1Rel, esr.norm1RelPosTags) filter filterTokens map stemToken
      val relString = relTokens.map(_.string).mkString(" ")
      if (relTokens.isEmpty || relString.startsWith("'")) None 
      else {
        val arg1Tokens = joinTokensAndPostags(esr.norm1Arg1, esr.norm1Arg1PosTags) filter filterTokens map stemToken
        val arg2Tokens = joinTokensAndPostags(esr.norm1Arg2, esr.norm1Arg2PosTags) filter filterTokens map stemToken
        Some(relString, ArgContext(arg1Tokens, arg2Tokens).toString)
      }
    } 
    catch { case e: Exception => { e.printStackTrace; None } }
  }
}