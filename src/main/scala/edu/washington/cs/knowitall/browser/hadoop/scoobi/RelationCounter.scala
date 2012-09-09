package edu.washington.cs.knowitall.browser.hadoop.scoobi

import scopt.OptionParser
import com.nicta.scoobi.Scoobi._
import util.ExtractionSentenceRecord
import edu.washington.cs.knowitall.tool.stem.MorphaStemmer
import edu.washington.cs.knowitall.tool.postag.PostaggedToken

import scala.collection.mutable

/**
 * Produces a list of id, relation string, frequency in descending order of frequency.
 */
object RelationCounter extends ScoobiApp {

  private val stemmerLocal = new ThreadLocal[MorphaStemmer] { override def initialValue = new MorphaStemmer }
  def stemmer = stemmerLocal.get
  
  case class Relation(val tokens: Seq[PostaggedToken]) {
    override def toString = tokens.map { token => "%s_%s".format(token.string, token.postag) }.mkString(" ")
    def auxString: String = Seq(tokens.map(_.string).mkString(" "), tokens.map(_.postag).mkString(" ")).mkString("\t")
  }
  object Relation {
    def fromString(relString: String): Option[Relation] = {
      try {
        val tokens = relString.split(" ").map { relSplit =>
          relSplit.split("_") match {
            case Array(string, postag) => new PostaggedToken(postag, string, 0)
            case _ => return None
          }
        }
        Some(Relation(tokens))
      } catch {
        case e: Exception => { e.printStackTrace; None }
      }
    }
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
    
    val relations = input flatMap toRelationArgs
    
    val grouped = relations.groupByKey

    val tabulated = grouped map tabulateGroup
    
    persist(toTextFile(tabulated, outputPath + "/"))
  }
  
  val badTokens = Set("a", "an", "the")
  
  def filterTokens(token: PostaggedToken) = !badTokens.contains(token.string)
  
  def filterArgString(argString: String): Boolean = {
    argString.length >= 2 && argString.length < 30
  }
  
  // returns (Relation.doubleString, most frequent arg1s, most frequent arg2s)
  def tabulateGroup(group: (String, Iterable[(String, String)])): String = group match { case (relString, argStrings) =>
    val arg1Counts = new mutable.HashMap[String, MutableInt]
    val arg2Counts = new mutable.HashMap[String, MutableInt]
    
    val rel = Relation.fromString(relString).get
    var size = 0
    
    argStrings.iterator.foreach { case (arg1, arg2) =>
      size += 1
      val sizeOk = size < 200000
      if (sizeOk && filterArgString(arg1)) arg1Counts.getOrElseUpdate(arg1, MutableInt(0)).inc
      if (sizeOk && filterArgString(arg2)) arg2Counts.getOrElseUpdate(arg2, MutableInt(0)).inc
    }
    
    def mostFrequent(counts: mutable.Map[String, MutableInt]): Seq[String] = {
      counts.iterator.filter({ case (string, mutFreq) => mutFreq.value > 1 }).toSeq.sortBy(-_._2.value).map(_._1).take(6)
    }
    
    Seq(size, rel.auxString, mostFrequent(arg1Counts).mkString(", "), mostFrequent(arg2Counts).mkString(", ")).mkString("\t")
  }
  
  // (Relation.toString, (arg1String, arg2String))
  def toRelationArgs(inputRecord: String): Option[(String, (String, String))] = {
    try { 
      val esr = new ExtractionSentenceRecord(inputRecord)
      val relTokens = esr.norm1Rel.split(" ").map(_.toLowerCase)
      val relPos = esr.norm1RelPosTags.split(" ")
      val posTokens = relTokens.zip(relPos) map { case (tok, pos) => new PostaggedToken(pos, tok, 0) } filter filterTokens
      if (posTokens.isEmpty) None else Some((Relation(posTokens).toString, (esr.arg1, esr.arg2)))
    } 
    catch { case e: Exception => { e.printStackTrace; None }}
  }
  
  // Utility Hacks for counting a long list of strings
  
  case class MutableInt(var value: Int) {
    def inc: Unit = { value += 1 }
  }
}

