package edu.washington.cs.knowitall.browser.entity

import edu.washington.cs.knowitall.browser.hadoop.scoobi.ScoobiEntityLinker
import edu.washington.cs.knowitall.browser.hadoop.util.FbTypeLookup

import scala.collection.JavaConversions._

/**
  * Does type lookup for freebase entities (fills in the argXTypes field in an extractionGroup)
  */
class EntityTyper {

  def getRandomElement[T](seq: Seq[T]): T = seq(scala.util.Random.nextInt(seq.size))

  lazy val fbEntityIndexes = ScoobiEntityLinker.getScratch("browser-freebase/type-lookup-index/")
  lazy val fbTypeEnumFile = "/scratch/browser-freebase/fbTypeEnum.txt"

  private lazy val fbLookupTables = fbEntityIndexes.map(index => new FbTypeLookup(index, fbTypeEnumFile))

  /**
   * mutator method to 
   */
  def typeEntity(entity: Entity): Entity = {
    
    val fbid = entity.fbid

    val types = getRandomElement(fbLookupTables).getTypesForEntity(entity.fbid)
    
    entity.attachTypes(types)

    return entity
  }
}
object EntityTyper {

  val typerLocal = new ThreadLocal[EntityTyper]() { override def initialValue() = new EntityTyper() }
  
}