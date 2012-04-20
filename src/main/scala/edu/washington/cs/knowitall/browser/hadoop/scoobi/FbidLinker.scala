package edu.washington.cs.knowitall.browser.hadoop.scoobi

  import com.nicta.scoobi.Scoobi._
  import com.nicta.scoobi.DList._
  import com.nicta.scoobi.DList
  import com.nicta.scoobi.io.text.TextInput._
  import com.nicta.scoobi.io.text.TextInput
  import com.nicta.scoobi.io.text.TextOutput._
  import com.nicta.scoobi.io.text.TextOutput
  
  import java.io.File
  import java.io.FileWriter
  
  import scala.collection.JavaConversions._
  
  import edu.washington.cs.knowitall.browser.extraction.ReVerbExtraction
  import edu.washington.cs.knowitall.browser.extraction.ExtractionGroup
  import edu.washington.cs.knowitall.browser.hadoop.entity.TopCandidatesFinder
  import edu.washington.cs.knowitall.browser.hadoop.entity.EntityLinker
  
class FbidLinker(val el: EntityLinker) {

  case class RVTuple(arg1: String, rel: String, arg2: String) {
    def toString = "%s, %s, %s".format(arg1, rel, arg2)
  }
  
  def listWrapper(inList : java.util.List[String]): Iterable[String] = {
    
    inList.toIterable
  }
  
  // returns an (arg1, rel, arg2) tuple of normalized string tokens
  def getNormalizedKey(extr: ReVerbExtraction): RVTuple = {
    RVTuple("edison", "invent", "internet")
  }
  
  def getKeyValuePair(line: String): Option[(String, String)] = {
    // parse the line to a ReVerbExtraction
      val extrOpt = ReVerbExtraction.fromTabDelimited(line.split("\t"))._1
      
      extrOpt match {
        case Some(extr) => Some((getNormalizedKey(extr).toString, line))
        case None => None
      }
  }
  
  def processGroup(key: String, rawExtrs: Iterable[String]): Option[ExtractionGroup[ReVerbExtraction]] = {
    
    def failure(msg: String = "") = {
      System.err.println("Error processing in processGroup: "+msg+", key: "+key);
      rawExtrs.foreach(str => System.err.println(str))
      None
    }
    
    val extrs = rawExtrs.flatMap(line => ReVerbExtraction.fromTabDelimited(line.split("\t"))._1)
    
    val rvtuple = getNormalizedKey(extrs.head)
    
    if (rvtuple.toString.equals(key)) return failure("Key Mismatch: "+rvtuple.toString+" != "+key)
      
    val arg1Entity = el.linkAndStuff()
    
    
    None
  }
  
  def main(args: Array[String]) = withHadoopArgs(args) { a =>

    val trans = new TopCandidatesFinder()
    
    val (inputPath, outputPath) = (a(0), a(1))

    val lines: DList[String] = TextInput.fromTextFile(inputPath)

    // first we need to define what will be our key. We strip only articles - everything else just gets normalized... 
    // normalized by something that takes POS tags, so it performs best.
    val keyValuePair: DList[(String, String)] = lines.flatMap { line => getKeyValuePair(line) }
    
    val groupedByKey = keyValuePair.groupByKey
    

    DList.persist(TextOutput.toTextFile(keyValuePair, outputPath + "/test-results"));
  }  
}