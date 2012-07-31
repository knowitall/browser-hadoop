package edu.washington.cs.knowitall.browser.hadoop.scoobi.util

import edu.washington.cs.knowitall.browser.extraction.ReVerbExtraction
import edu.washington.cs.knowitall.browser.extraction.FreeBaseEntity
import edu.washington.cs.knowitall.browser.extraction.FreeBaseType
import edu.washington.cs.knowitall.browser.extraction.Instance
import edu.washington.cs.knowitall.browser.extraction.ExtractionTuple

sealed abstract class ArgField {
  
  def name: String
  def getArgNorm[T <: ExtractionTuple](reg: T): String
  def getTypeStrings[T <: ExtractionTuple](reg: T): Set[String]
  def attachTypes[T <: ExtractionTuple](reg: T, typeInts: Seq[Int]): T
  def loadEntityInfo[T <: ExtractionTuple](group: T): Option[EntityInfo]
  protected def fbTypeToString(fbType: FreeBaseType): String = "/%s/%s".format(fbType.domain, fbType.typ)
  // Silently returns none
  protected def intToFbType(typeInt: Int): Option[FreeBaseType] = {
    val typeInfo = TypeInfoUtils.typeEnumMap.get(typeInt).getOrElse { return None }
    FreeBaseType.parse(typeInfo.typeString)
  }
  // reports return of None to stderr
  protected def intToFbTypeVerbose(typeInt: Int): Option[FreeBaseType] = {
    val fbTypeOpt = intToFbType(typeInt)
    if (!fbTypeOpt.isDefined) System.err.println("Couldn't parse type int: %d".format(typeInt))
    fbTypeOpt
  }
  protected def typeToInt(typ: String): Int = TypeInfoUtils.typeStringMap(typ).enum

}

case class Arg1() extends ArgField {
  override val name = "arg1"
  override def getArgNorm[T <: ExtractionTuple](reg: T) = reg.arg1.norm
  override def getTypeStrings[T <: ExtractionTuple](reg: T) = reg.arg1.types map fbTypeToString filter TypeInfoUtils.typeFilter
  override def attachTypes[T <: ExtractionTuple](reg: T, typeInts: Seq[Int]) = reg.setArg1(reg.arg1.copy(types = typeInts flatMap intToFbType toSet)).asInstanceOf[T]
  override def loadEntityInfo[T <: ExtractionTuple](reg: T): Option[EntityInfo] = reg.arg1.entity map { e =>
    EntityInfo(e.fbid, getTypeStrings(reg) map typeToInt)
  }
}
case class Arg2() extends ArgField {
  override val name = "arg2"
  override def getArgNorm[T <: ExtractionTuple](reg: T) = reg.arg2.norm
  override def getTypeStrings[T <: ExtractionTuple](reg: T) = reg.arg2.types map fbTypeToString filter TypeInfoUtils.typeFilter
  override def attachTypes[T <: ExtractionTuple](reg: T, typeInts: Seq[Int]) = reg.setArg2(reg.arg2.copy(types = typeInts flatMap intToFbType toSet)).asInstanceOf[T]
  override def loadEntityInfo[T <: ExtractionTuple](reg: T): Option[EntityInfo] = reg.arg2.entity map { e =>
    EntityInfo(e.fbid, getTypeStrings(reg) map typeToInt)
  }
}