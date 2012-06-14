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

  val numGroupsToTest = 2000

  var inputLines: List[String] = Source.fromInputStream(ResourceUtils.loadResource("test-groups-5.txt", this.getClass()), "UTF-8").getLines.drop(1000).take(numGroupsToTest).toList

  private def getExtrsHelper = inputLines.flatMap(e => ReVerbExtractionGroup.fromTabDelimited(e.split("\t"))._1)

  val stemmer = new MorphaStemmer

  @Test
  def testBuildIndex: Unit = {



    val ramDir = new RAMDirectory()

    val indexWriter = new IndexWriter(ramDir, ReVerbIndexBuilder.indexWriterConfig(ramBufferMB=10))

    val indexBuilder = new IndexBuilder(indexWriter, ReVerbIndexBuilder.inputLineConverter(regroup=false), 100)

    System.err.println("Building test index...")

    // build the index
    indexBuilder.indexAll(inputLines.iterator)

    indexWriter.close

    System.err.println("Finished building test index, running test queries")

    // open the index and try to read from it
    val indexReader = IndexReader.open(ramDir)
    val indexSearcher = new IndexSearcher(indexReader)
    val fetcher = new ExtractionGroupFetcher(indexSearcher, 1000, 1000, 1000, Set.empty)

    // test that each input group can be found in the index
    def testGroup(group: ExtractionGroup[ReVerbExtraction]): Unit = {
      val resultGroups = fetcher.getGroups(group.identityQuery)
      if (!resultGroups.results.toSet.contains(group)) {
        println(); println()
        println("Expected: %s".format(ReVerbExtractionGroup.toTabDelimited(group)))

        println("Found: ")
        resultGroups.results.foreach { resultGroup =>
          println(ReVerbExtractionGroup.toTabDelimited(group))
        }
        fail()
      }
    }

    getExtrsHelper.foreach(testGroup(_))

    System.err.println("Test queries pass")

    ramDir.close
    indexReader.close
    indexSearcher.close
  }
}
