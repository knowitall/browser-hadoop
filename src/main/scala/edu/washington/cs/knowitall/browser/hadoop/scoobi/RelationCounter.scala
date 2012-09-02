package edu.washington.cs.knowitall.browser.hadoop.scoobi

import scopt.OptionParser
import com.nicta.scoobi.Scoobi._
import util.ExtractionSentenceRecord

/**
 * Produces a list of id, relation string, frequency in descending order of frequency.
 */
object RelationCounter extends ScoobiApp {

  case class CountedRelation(val rel: String, val freq: Int) {
    override def toString = Seq(rel, freq).mkString("\t")
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

    val relationsCounted = relationsGrouped.map { case (rel, rels) => CountedRelation(rel, rels.size) } filter { _.freq >= minFrequency} map { _.toString }
    
    persist(toTextFile(relationsCounted, outputPath + "/"))
  }
  
  def toRelationString(inputRecord: String): Option[String] = {
    try { return Some(new ExtractionSentenceRecord(inputRecord).rel) } 
    catch { case e: Exception => { e.printStackTrace; None }}
  }
}
