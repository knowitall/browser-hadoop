package edu.washington.cs.knowitall.browser.hadoop.scoobi

import scopt.OptionParser
import com.nicta.scoobi.Scoobi._
import util.ExtractionSentenceRecord
import edu.washington.cs.knowitall.tool.stem.MorphaStemmer
import edu.washington.cs.knowitall.tool.postag.PostaggedToken

/**
 * Produces a list of id, relation string, frequency in descending order of frequency.
 */
object RelationCounter extends ScoobiApp {

  private val stemmerLocal = new ThreadLocal[MorphaStemmer] { override def initialValue = new MorphaStemmer }
  def stemmer = stemmerLocal.get
  
  case class CountedRelation(val rel: String, val postags: String, val freq: Int) {
    override def toString = Seq(freq, rel, postags).mkString("\t")
  }
  
  def run(): Unit = {

    var inputPath = ""
    var outputPath = ""
    var minFrequency = 1
      
    val parser = new OptionParser() {
      arg("inputPath", "file input path, records delimited by newlines", { str => inputPath = str })
      arg("outputPath", "file output path, newlines again", { str => outputPath = str })
      intOpt("minFreq", "don't keep relations below this frequency", { num => minFrequency = num })
    }
    
    if (!parser.parse(args)) return

    println("Parsed args: %s".format(args.mkString(" ")))
    println("inputPath=%s".format(inputPath))
    println("outputPath=%s".format(outputPath))
    
    // serialized ReVerbExtractions
    val input: DList[String] = fromTextFile(inputPath)
    
    val relations = input flatMap toRelationString
    
    val relationsGrouped = relations.groupBy(identity)

    val relationsCounted = relationsGrouped.map { case ((rel, postags), rels) => CountedRelation(rel, postags, rels.size) } filter { _.freq >= minFrequency} map { _.toString }
    
    persist(toTextFile(relationsCounted, outputPath + "/"))
  }
  
  val badTokens = Set("a", "an", "the")
  
  def filterTokens(token: PostaggedToken) = !badTokens.contains(token.string)
  
  def toRelationString(inputRecord: String): Option[(String, String)] = {
    try { 
      val esr = new ExtractionSentenceRecord(inputRecord)
      val relTokens = esr.rel.split(" ").map(_.toLowerCase)
      val relPos = esr.relTag.split(" ")
      val posTokens = relTokens.zip(relPos) map { case (tok, pos) => new PostaggedToken(pos, tok, 0) } filter filterTokens
      val stemmed = posTokens map stemmer.stemToken
      val result = stemmed.map(tok => (tok.lemma, tok.token.postag))
      val rel = result.map(_._1).mkString(" ")
      val postags = result.map(_._2).mkString(" ")
      if (result.isEmpty) None else Some((rel, postags))
    } 
    catch { case e: Exception => { e.printStackTrace; None }}
  }
}
