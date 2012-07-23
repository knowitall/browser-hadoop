package edu.washington.cs.knowitall.browser.hadoop.scoobi.util

import edu.washington.cs.knowitall.browser.hadoop.scoobi.UnlinkableEntityTyper.REG
import edu.washington.cs.knowitall.browser.extraction.ReVerbExtraction
import edu.washington.cs.knowitall.browser.extraction.FreeBaseEntity
import edu.washington.cs.knowitall.browser.extraction.FreeBaseType
import edu.washington.cs.knowitall.browser.extraction.Instance
import edu.washington.cs.knowitall.browser.extraction.ExtractionGroup

sealed abstract class ArgField {
  def name: String
  def getArgNorm(reg: REG): String
  def getTypeStrings(reg: REG): Set[String]
  def attachTypes(reg: REG, typeInts: Seq[Int]): REG
  def loadEntityInfo(group: REG): Option[EntityInfo]
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
  override def getArgNorm(reg: REG) = reg.arg1.norm
  override def getTypeStrings(reg: REG) = reg.arg1.types map fbTypeToString filter TypeInfoUtils.typeFilter
  override def attachTypes(reg: REG, typeInts: Seq[Int]) = reg.copy(arg1 = reg.arg1.copy(types = typeInts flatMap intToFbType toSet))
  override def loadEntityInfo(reg: REG): Option[EntityInfo] = reg.arg1.entity map { e =>
    EntityInfo(e.fbid, getTypeStrings(reg) map typeToInt)
  }
}
case class Arg2() extends ArgField {
  override val name = "arg2"
  override def getArgNorm(reg: REG) = reg.arg2.norm
  override def getTypeStrings(reg: REG) = reg.arg2.types map fbTypeToString filter TypeInfoUtils.typeFilter
  override def attachTypes(reg: REG, typeInts: Seq[Int]) = reg.copy(arg2 = reg.arg2.copy(types = typeInts flatMap intToFbType toSet))
  override def loadEntityInfo(reg: REG): Option[EntityInfo] = reg.arg2.entity map { e =>
    EntityInfo(e.fbid, getTypeStrings(reg) map typeToInt)
  }
}