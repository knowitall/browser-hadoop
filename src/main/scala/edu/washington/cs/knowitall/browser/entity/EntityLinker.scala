package edu.washington.cs.knowitall.browser.entity

import java.io.FileNotFoundException
import java.io.IOException
import java.util.ArrayList
import java.util.List

import org.apache.lucene.index.CorruptIndexException

import scala.collection.JavaConversions._

import edu.washington.cs.knowitall.browser.hadoop.scoobi.ScoobiEntityTyper

class EntityLinker(val bm: batch_match, val oneT: TopCandidatesFinder) {

  private val PAD_SOURCES = 4; // extend source sentences to this
  // number minimum

  private var totalLookups = 0
  private var cacheHits = 0
  private var cacheTimeouts = 0

  def this(contextSimIndex: String) = this(new batch_match(contextSimIndex), new TopCandidatesFinder())

  private def tryFbidCache(arg: String): Seq[String] = oneT.linkToFbids(arg)

  def getBestEntity(arg: String, sourceSentences: Seq[String]): Entity = {
    
    val entityNoTypes = getBestFbidFromSources(arg, sourceSentences)
    
    if (entityNoTypes == null) return null
    
    val typedEntity = ScoobiEntityTyper.typerLocal.get.typeEntity(entityNoTypes)
    
    return typedEntity
  }
  
  /**
    * returns null for none! Returns an entity without types attached.
    *
    * @param arg
    * @param sources
    * @return
    * @throws IOException
    * @throws ClassNotFoundException
    */
  private def getBestFbidFromSources(arg: String, inputSources: Seq[String]): Entity = {

    var sources = inputSources

    if (sources.isEmpty()) {
      System.err.println("Warning: no source sentences for arg: " + arg);
      return null; // later code assumes that we don't have any empty list
    }
    totalLookups += 1
    val fbids = tryFbidCache(arg)

    if (totalLookups % 20000 == 0)
      System.err.println("Linker lookups: " + totalLookups
        + " cache hits: " + cacheHits + " cache timeouts: "
        + cacheTimeouts)

    if (fbids.isEmpty()) return null

    while (sources.size() < PAD_SOURCES) {
      val newSources = new ArrayList[String](PAD_SOURCES)
      newSources.addAll(sources);

      for (s <- sources)
        if (newSources.size() < PAD_SOURCES)
          newSources.add(s)

      sources = newSources;
    }

    val fbidScores = bm.processSingleArgWithSources(arg, Indices.convertFbids(fbids), sources).toIterable

    return getBestFbid(arg, fbidScores);
  }

  /**
    * Return (title, fbid) for the best entity match,
    *
    * returns null for none
    *
    * @param arg1
    * @param fbidScores
    * @return
    * @throws ClassNotFoundException
    * @throws IOException
    * @throws FileNotFoundException
    */
  private def getBestFbid(arg: String, fbidScores: Iterable[Pair[String, java.lang.Double]]): Entity = {

    var bestScore = Double.NegativeInfinity
    var bestTitle = ""
    var bestFbid = ""
    var bestInlinks = 0

    var fbidScoresEmpty = true

    for (fbidScore <- fbidScores) {
      fbidScoresEmpty = false;
      val titleInlinks = oneT.getTitleInlinks(fbidScore.one)
      val title = titleInlinks.one
      val inlinks = titleInlinks.two

      val thisScore = scoreFbid(arg, title, inlinks, fbidScore.two)
      if (thisScore > bestScore) {
        bestScore = thisScore;
        bestInlinks = inlinks;
        bestTitle = title;
        bestFbid = fbidScore.one;
      }
    }

    if (bestTitle.isEmpty() && !fbidScoresEmpty)
      throw new RuntimeException(
        "There should have been a FB match here, implementation error.");

    if (bestTitle.isEmpty()) {
      return null;
    } else {
      return new Entity(bestTitle, bestFbid, bestScore, bestInlinks);
    }

  }

  private def scoreFbid(arg: String, title: String,
    inlinks: Int, score: Double): Double = {

    val string_match_level =
      if (arg.equalsIgnoreCase(title))
        5
      else {
        // compute what tom puts in his "sml" array for similarity
        val entity_parts = title.split(" ")
        val arg_parts = arg.split(" ")
        val word_diff = entity_parts.length - arg_parts.length
        4 - word_diff
      }
    val sml = math.max(1.0, string_match_level)

    val score2 = math.log(inlinks) * score * sml
    return score2
  }

}
