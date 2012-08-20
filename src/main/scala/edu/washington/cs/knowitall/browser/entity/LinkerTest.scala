package edu.washington.cs.knowitall.browser.entity

import edu.washington.cs.knowitall.browser.hadoop.scoobi.ScoobiEntityLinker
import edu.washington.cs.knowitall.browser.extraction._
import scala.collection.mutable

class LinkerTest {

  type REG = ExtractionGroup[ReVerbExtraction]
  
  case class ExtractionPartStats(
    var beforeLinks: Int,
    var afterLinks: Int,
    var changedLinks: Int,
    var newLinks: Int,
    var lostLinks: Int) {
    
    val changeEvidence = new mutable.HashSet[(FreeBaseEntity, FreeBaseEntity)]
    val lostEvidence = new mutable.HashSet[FreeBaseEntity]
    val newEvidence = new mutable.HashSet[FreeBaseEntity]
    
    def register(beforePart: ExtractionPart, afterPart: ExtractionPart): Unit = {
      val bef = beforePart.entity.isDefined
      val aft = afterPart.entity.isDefined
      if (bef) beforeLinks += 1
      if (aft) afterLinks += 1
      if (!bef && aft) { newLinks += 1; newEvidence += afterPart.entity.get }
      else if (bef && !aft) { lostLinks += 1; lostEvidence += beforePart.entity.get }
      
      if (bef && aft && !beforePart.entity.get.fbid.equals(afterPart.entity.get.fbid)) {
        changedLinks += 1
        changeEvidence += ((beforePart.entity.get, afterPart.entity.get))
      }
    }
    
    def evidenceString: String = {
      "NewLinks: %s\n".format(newEvidence.map(_.name)) +
      "LostLinks: %s\n".format(lostEvidence.map(_.name)) +
      "ChangeLinks: %s".format(changeEvidence.map(pair => "%s -> %s".format(pair._1.name, pair._2.name)))
    }
    
    override def toString: String = "Before: %d, After: %d, Changed: %d, New: %d, Lost: %d".format(beforeLinks, afterLinks, changedLinks, newLinks, lostLinks)
  }
  
  object stats {
    var totalRegs = 0
    var arg1 = ExtractionPartStats(0,0,0,0,0)
    var arg2 = ExtractionPartStats(0,0,0,0,0)
    
    def register(before: REG, after: REG): Unit = {
      totalRegs += 1
      arg1.register(before.arg1, after.arg1)
      arg2.register(before.arg2, after.arg2)
    }
    
    override def toString: String = {
      val headerString = "TotalGroups: %d\nArg1Links:[%s]\nArg2Links:[%s]".format(totalRegs, arg1.toString, arg2.toString)
      val evidenceString = "Arg1 Evidence:\n%s\nArg2 Evidence:\n%s".format(arg1.evidenceString, arg2.evidenceString)
      "%s\n%s".format(headerString, evidenceString)
    }
  }
  
  val linker = ScoobiEntityLinker.getEntityLinker(1)
  
  def runTest(inputRegs: Iterable[REG]): Unit = {
    
    inputRegs.foreach { before =>
      println(ReVerbExtractionGroup.serializeToString(before))
      val after = linker.linkEntities(reuseLinks = false)(before)
      stats.register(before, after)
       println(ReVerbExtractionGroup.serializeToString(after))
    }
    println(stats)
  }
}

object LinkerTest {
  
  import scopt.OptionParser
  import scala.io.Source
  
  def main(args: Array[String]): Unit = {
    
    var inputFiles: Seq[String] = Nil
    var maxGroups: Int = Int.MaxValue
    
    val parser = new OptionParser("LinkerTest") {
      arg("inputFiles", "comma-separated input files to read groups from", { str => inputFiles = str.split(",") })
      intOpt("maxGroups", "maximum groups to test per input file", { i => maxGroups = i })
    }
    
    if (!parser.parse(args)) return
    
    val sources = inputFiles.map { file => Source.fromFile(file) }
    val inputRegs = sources.flatMap { source => source.getLines flatMap ReVerbExtractionGroup.deserializeFromString take(maxGroups) }
    sources.foreach { _.close } 
    (new LinkerTest).runTest(inputRegs)
  }
}