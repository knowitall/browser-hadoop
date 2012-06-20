package edu.washington.cs.knowitall.browser.entity

import edu.washington.cs.knowitall.browser.hadoop.scoobi.ScoobiEntityLinker
import edu.washington.cs.knowitall.browser.hadoop.util.FbTypeLookup

import scala.collection.JavaConversions._

/**
  * Does type lookup for freebase entities (fills in the argXTypes field in an extractionGroup)
  */
class EntityTyper(val fbLookupTable: FbTypeLookup) {

  def this(basePath: String) = this(new FbTypeLookup(basePath+EntityTyper.typeLookupIndex, basePath+EntityTyper.fbTypeEnumFile))

  /**
   * mutator method to 
   */
  def typeEntity(entity: Entity): Entity = {
    
    val fbid = entity.fbid

    val types = fbLookupTable.getTypesForEntity(entity.fbid)
    
    entity.attachTypes(types)

    return entity
  }
}

object EntityTyper {
  val typeLookupIndex = "type-lookup-index/"
  val fbTypeEnumFile = "fbTypeEnum.txt"
}