package edu.washington.cs.knowitall.browser.lucene

import org.apache.lucene.index.IndexReader
import org.apache.lucene.index.IndexWriter
import edu.washington.cs.knowitall.browser.extraction.ExtractionGroup
import edu.washington.cs.knowitall.browser.extraction.ReVerbExtractionGroup
import edu.washington.cs.knowitall.browser.extraction.ReVerbExtraction

import edu.washington.cs.knowitall.browser.hadoop.scoobi.ScoobiEntityLinker

import org.apache.lucene.search.IndexSearcher


/**
 * Adds collections of unlinked ReVerb ExtractionGroups to an existing index,
 * such as an index created by IndexBuilder.
 * 
 * Command-line interface allows the user to skip linking for singleton groups
 * that do not join any new group in the index (this saves a lot of time)
 */
class ReVerbIndexModifier(val fetcher: ExtractionGroupFetcher, val writerBufferMb: Int, val groupsPerCommit: Int) {

  val searcher = fetcher.indexSearcher
  val reader = fetcher.indexSearcher.getIndexReader
  val writer = new IndexWriter(reader.directory, ReVerbIndexBuilder.indexWriterConfig(writerBufferMb))
  val linker = ScoobiEntityLinker.getEntityLinker
  
  type REG = ExtractionGroup[ReVerbExtraction]

  /**
    * Updates group to the index. Returns true if this is a new group (by key) to the index
    * User can specify to only add if the group is already in the index
    * (will be useful for parallel indexes)
    */
  private def updateGroup(group: REG, onlyIfAlreadyExists: Boolean): Boolean = {

    val querySpec = group.identityQuery
    val beforeGroups = fetcher.getGroups(querySpec) match {
      case Timeout(results, _) => throw new RuntimeException("Failed to add document due to timeout.")
      case Limited(results, _) => throw new RuntimeException("Index results were limited... this shouldn't happen!")
      case Success(results) => results
    }

    if (!onlyIfAlreadyExists || !beforeGroups.isEmpty) {

      // delete matching documents from the index
      writer.deleteDocuments(querySpec.luceneQuery)

      // now we have the existing groups in memory. We just need to add "group" and then merge
      val afterGroups = group :: beforeGroups

      val key = (group.arg1.norm, group.rel.norm, group.arg2.norm)
      // merge them:
      val mergedGroup = ReVerbExtractionGroup.mergeGroups(key, afterGroups)

      val size = mergedGroup.instances.size

      var linkedGroup = mergedGroup

      // re-run the linker only if the right conditions hold
      if (size == 1 || (size > 10 && size % 2 == 0)) {
        
        linkedGroup = linker.linkEntities(mergedGroup, reuseLinks = false)
      }

      val document = ReVerbDocumentConverter.toDocument(linkedGroup)
      writer.addDocument(document)

      return beforeGroups.isEmpty
    } else {
      false
    }
  }
  
  def updateAll(groups: Iterable[REG]): Unit = {
    
    var groupsProcessed = 0
    
    groups.grouped(groupsPerCommit).foreach { groupOfGroups =>
      groupOfGroups.foreach(updateGroup(_, false))
      groupsProcessed += groupOfGroups.size
      writer.commit()
      System.err.println("Groups inserted: %d".format(groupsProcessed))
      
    }
  }
}
