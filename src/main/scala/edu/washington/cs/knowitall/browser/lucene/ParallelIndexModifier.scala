package edu.washington.cs.knowitall.browser.lucene

import edu.washington.cs.knowitall.browser.extraction.ExtractionGroup
import edu.washington.cs.knowitall.browser.extraction.ReVerbExtractionGroup
import edu.washington.cs.knowitall.browser.extraction.ReVerbExtraction

class ParallelReVerbIndexModifier(val basicModifiers: Seq[ReVerbIndexModifier], groupsPerCommit: Int) extends IndexModifier {

  def fetcher = new ParallelExtractionGroupFetcher(basicModifiers.map(_.fetcher))
  
  private def updateGroup(group: REG): Boolean = {
    
    for (modifier <- basicModifiers) {
      val added = modifier.updateGroup(group, onlyIfAlreadyExists=true)
      if (added) {
        println("UPDATED")
        return true
      }
    }
    return false
  }
  
  private def addToRandomGroup(group: REG): Unit = {
    
    val randomModifier = basicModifiers(scala.util.Random.nextInt(basicModifiers.length))
    randomModifier.addGroup(group, true)
    println("ADDED")
  }
  
  def updateAll(groups: Iterable[REG]): Unit = {
    
    var groupsProcessed = 0
    
    groups.grouped(groupsPerCommit).foreach { groupOfGroups =>
      
      groupOfGroups.foreach { group => 
        val updated = updateGroup(group)
        if (!updated) addToRandomGroup(group)
      }
      
      groupsProcessed += groupOfGroups.size
      basicModifiers map(_.writer.commit())
      System.err.println("Groups inserted: %d".format(groupsProcessed))
      
    }
  }
}