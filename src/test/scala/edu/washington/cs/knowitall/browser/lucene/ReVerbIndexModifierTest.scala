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

  val numGroupsToTest = 1000                     

  var rawInputLines: List[String] = Source.fromInputStream(ResourceUtils.loadResource("test-groups-5.txt", this.getClass()), "UTF-8").getLines.drop(1000).take(numGroupsToTest).toList

  val inputLines =  rawInputLines flatMap lineToOptGroup flatMap(_.reNormalize) map ReVerbExtractionGroup.toTabDelimited
  
  private def lineToOptGroup(e: String) = ReVerbExtractionGroup.fromTabDelimited(e.split("\t"))._1

  val stemmer = new MorphaStemmer
  
  @Test
  def testModifyIndex: Unit = {
    
    val ramDir = new RAMDirectory()

    var indexWriter = new IndexWriter(ramDir, ReVerbIndexBuilder.indexWriterConfig(ramBufferMB=10))
    
    val indexBuilder = new IndexBuilder(indexWriter, ReVerbIndexBuilder.inputLineConverter(regroup=false), 100)

    val randomizedLines = inputLines //scala.util.Random.shuffle(inputLines)
    val firstHalfLines = randomizedLines.take(numGroupsToTest/2)
    val secondHalfLines  = randomizedLines.drop(numGroupsToTest/2)
    
    firstHalfLines foreach println
    secondHalfLines foreach println
    
    System.err.println("Building first half of index:")

    // build the index
    indexBuilder.indexAll(firstHalfLines.iterator)

    System.err.println("Finished building first half, adding second half...")

    indexWriter.close
    
    // open the index and try to read from it
    var indexReader = IndexReader.open(ramDir)
    var indexSearcher = new IndexSearcher(indexReader)
    var fetcher = new ExtractionGroupFetcher(indexSearcher, 10000, 10000, 100000, Set.empty)
    
    val indexModifier = new ReVerbIndexModifier(fetcher, None, 10, numGroupsToTest/4)
    
    indexModifier.updateAll(secondHalfLines flatMap lineToOptGroup)
    
    indexModifier.writer.commit
    
    indexReader = IndexReader.open(indexModifier.writer, true)
    System.err.println("MaxDoc=%d".format(indexReader.maxDoc))
    indexSearcher = new IndexSearcher(indexReader)
    fetcher = new ExtractionGroupFetcher(indexSearcher, 10000, 10000, 10000, Set.empty)
    
    System.err.println("Finished adding seconf half. Running test for first half:")
    // test that each input group can be found in the index
    def testGroup(group: ExtractionGroup[ReVerbExtraction]): Unit = {
      val resultGroups = fetcher.getGroups(group.identityQuery)
      if (!resultGroups.results.toSet.contains(group)) {
        println(); println()
        println("Expected: %s".format(ReVerbExtractionGroup.toTabDelimited(group)))

        println("Found: (%d, %d)".format(resultGroups.numGroups, resultGroups.numInstances))
        resultGroups.results.foreach { resultGroup =>
          println(ReVerbExtractionGroup.toTabDelimited(resultGroup))
        }
        fail()
      }
    }
    
    secondHalfLines flatMap lineToOptGroup foreach testGroup

    System.err.println("Second half test queries pass")
    
    firstHalfLines flatMap lineToOptGroup foreach testGroup
    
    System.err.println("First half test queries pass, running second half")

    

    
    
    
    
    ramDir.close
    indexReader.close
    indexSearcher.close
  }
}
