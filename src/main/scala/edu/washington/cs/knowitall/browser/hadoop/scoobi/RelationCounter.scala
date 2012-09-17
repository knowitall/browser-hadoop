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
    def tokenString: String = tokens.map(_.string).mkString(" ")
    def postagString: String = tokens.map(_.postag).mkString(" ")
    def auxString: String = Seq(tokenString, postagString).mkString("\t")
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
    var maxArgStringsPerRel = 6
      
    val parser = new OptionParser() {
      arg("inputPath", "file input path, records delimited by newlines", { str => inputPath = str })
      arg("outputPath", "file output path, newlines again", { str => outputPath = str })
      intOpt("minFreq", "don't keep relations below this frequency", { num => minFrequency = num })
      intOpt("maxArgs", "maximum top arg strings to show per rel", { num => maxArgStringsPerRel = num })
    }
    
    if (!parser.parse(args)) return

    println("Parsed args: %s".format(args.mkString(" ")))
    println("inputPath=%s".format(inputPath))
    println("outputPath=%s".format(outputPath))
    
    // serialized ReVerbExtractions
    val input: DList[String] = fromTextFile(inputPath)
    
    val relations = input flatMap toRelationArgs
    
    val grouped = relations.groupByKey

    val tabulated = grouped flatMap tabulateGroup(minFrequency)
    
    persist(toTextFile(tabulated, outputPath + "/"))
  }
  
  val badTokens = Set("a", "an", "the")
  
  def filterTokens(token: PostaggedToken) = !badTokens.contains(token.string)
  
  def filterArgString(argString: String): Boolean = {
    argString.length >= 2 && argString.length < 30 && !argString.equalsIgnoreCase("it")
  }
  
  // returns (Relation.doubleString, most frequent arg1s, most frequent arg2s)
  def tabulateGroup(minFrequency: Int)(group: (String, Iterable[(String, String, String)])): Option[String] = group match { case (relTokens, infoStrings) =>
    val arg1Counts = new mutable.HashMap[String, MutableInt]
    val arg2Counts = new mutable.HashMap[String, MutableInt]
    val relStringCounts = new mutable.HashMap[String, MutableInt]
    
    //val rel = Relation.fromString(relString).getOrElse { return None }
    var size = 0
    
    infoStrings.iterator.foreach { case (relString, arg1, arg2) =>
      size += 1
      val sizeOk = size < 500000
      if (sizeOk) relStringCounts.getOrElseUpdate(relString, MutableInt(0)).inc
      if (sizeOk && filterArgString(arg1)) arg1Counts.getOrElseUpdate(arg1, MutableInt(0)).inc
      if (sizeOk && filterArgString(arg2)) arg2Counts.getOrElseUpdate(arg2, MutableInt(0)).inc
    }
    
    def mostFrequent(counts: mutable.Map[String, MutableInt]): Seq[String] = {
      counts.iterator.filter({ case (string, mutFreq) => mutFreq.value > 1 }).toSeq.sortBy(-_._2.value).map(_._1).take(maxArgStringsPerRel)
    }
    
    val rel = mostFrequent(relStringCounts).headOption.flatMap(Relation.fromString _).getOrElse { return None }
     
    if (size < minFrequency) return None
    else {
      val result = Seq(size, rel.auxString, mostFrequent(arg1Counts).mkString(", "), mostFrequent(arg2Counts).mkString(", ")).mkString("\t")
      Some(result)
    }
  }
  
  // (rel tokens, (rel.toString, arg1String, arg2String))
  def toRelationArgs(inputRecord: String): Option[(String, (String, String, String))] = {
    try { 
      val esr = new ExtractionSentenceRecord(inputRecord)
      val relTokens = esr.norm1Rel.split(" ").map(_.toLowerCase)
      val relPos = esr.norm1RelPosTags.split(" ")
      val posTokens = relTokens.zip(relPos) map { case (tok, pos) => new PostaggedToken(pos, tok, 0) } filter filterTokens
      val stemTokens = posTokens map stemmer.stemToken map { lemma => new PostaggedToken(lemma.token.postag, lemma.lemma, 0) }
      val rel = Relation(stemTokens)
      if (posTokens.isEmpty) None 
      else Some(rel.tokenString, (rel.toString, esr.norm1Arg1.toLowerCase, esr.norm1Arg2.toLowerCase))
    } 
    catch { case e: Exception => { e.printStackTrace; None }}
  }
  
  // Utility Hacks for counting a long list of strings
  
  case class MutableInt(var value: Int) {
    def inc: Unit = { value += 1 }
  }
}

