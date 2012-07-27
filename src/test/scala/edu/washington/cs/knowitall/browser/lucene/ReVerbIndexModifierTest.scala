package edu.washington.cs.knowitall.browser.lucene

import edu.washington.cs.knowitall.browser.extraction.ExtractionGroup
import edu.washington.cs.knowitall.browser.extraction.ReVerbExtraction
import edu.washington.cs.knowitall.browser.extraction.ReVerbExtractionGroup
import edu.washington.cs.knowitall.commonlib.ResourceUtils
import edu.washington.cs.knowitall.tool.stem.MorphaStemmer
import org.apache.lucene.index.IndexReader
import org.apache.lucene.index.IndexWriter
import org.apache.lucene.search.IndexSearcher
import org.apache.lucene.store.RAMDirectory
import org.junit.runner.RunWith
import org.junit.Test
import org.scalatest.junit.JUnitRunner
import org.scalatest.Suite
import scala.Option.option2Iterable
import scala.io.Source


@RunWith(classOf[JUnitRunner])
class ReVerbIndexModifierTest extends Suite {

  type REG = ExtractionGroup[ReVerbExtraction]
  
  val numGroupsToTest = 1000

  val rawInputLines: List[String] = Source.fromInputStream(ResourceUtils.loadResource("test-groups-5000.txt", this.getClass()), "UTF-8").getLines.drop(1000).take(numGroupsToTest).toList

  val inputLines =  rawInputLines flatMap lineToOptGroup flatMap(_.reNormalize) map ReVerbExtractionGroup.serializeToString
  
  private def lineToOptGroup(e: String) = ReVerbExtractionGroup.deserializeFromString(e)

  val stemmer = new MorphaStemmer
  
  @Test
  def testModifyIndex: Unit = {
    
    val ramDir = new RAMDirectory()

    var indexWriter = new IndexWriter(ramDir, ReVerbIndexBuilder.indexWriterConfig(ramBufferMB=10))
    
    val indexBuilder = new IndexBuilder(indexWriter, ReVerbIndexBuilder.inputLineConverter(regroup=false, filter = false), 100)

    val eachHalfSize = numGroupsToTest/2
    
    println("Halfsize: %d".format(eachHalfSize))
    
    val randomizedLines = inputLines //scala.util.Random.shuffle(inputLines)
    val firstHalfLines = randomizedLines.take(eachHalfSize)
    val secondHalfLines  = randomizedLines.drop(eachHalfSize)
    
    firstHalfLines foreach println
    println
    secondHalfLines foreach println
    
    val firstHalfGroups = firstHalfLines flatMap lineToOptGroup
    val secondHalfGroups = secondHalfLines flatMap lineToOptGroup
    
    System.err.println("Building first half of index:")

    // build the index
    indexBuilder.indexAll(firstHalfLines.iterator)
    var indexReader = IndexReader.open(indexWriter, true)
    System.err.println("Finished building first half (%d), adding second half...".format(indexReader.maxDoc))
    
    var indexSearcher = new IndexSearcher(indexReader)
    var fetcher = new ExtractionGroupFetcher(indexSearcher, 10000, 10000, 100000, Set.empty[String])
    
    testAll(fetcher, firstHalfGroups)
    
    val indexModifier = new ReVerbIndexModifier(indexWriter, None, 100, 100)
    
    indexModifier.updateAll(secondHalfGroups.iterator)
    
    testAll(indexModifier.fetcher, secondHalfGroups)
    testAll(indexModifier.fetcher, firstHalfGroups)
  }

  def testAll(fetcher: GroupFetcher, groups: Iterable[REG]): Unit = { groups.foreach(testGroup(fetcher, _)) }

  // test that each input group can be found in the index
  def testGroup(fetcher: GroupFetcher, group: ExtractionGroup[ReVerbExtraction]): Unit = {
    val query = QuerySpec.identityQuery(group)
    println(query.luceneQuery)
    val resultGroups = fetcher.getGroups(query)
    if (!resultGroups.results.toSet.contains(group)) {
      println(); println()
      println("Expected (%d): %s".format(group.instances.size, ReVerbExtractionGroup.serializeToString(group)))

      println("Found: (%d, %d)".format(resultGroups.numGroups, resultGroups.numInstances))
      resultGroups.results.foreach { resultGroup =>
        println(ReVerbExtractionGroup.serializeToString(resultGroup))
      }
      fail()
    }
  }
}
