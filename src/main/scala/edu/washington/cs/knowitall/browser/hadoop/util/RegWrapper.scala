package edu.washington.cs.knowitall.browser.hadoop.util

import edu.washington.cs.knowitall.browser.util.StringSerializer
import edu.washington.cs.knowitall.browser.extraction._
import edu.washington.cs.knowitall.browser.hadoop.scoobi.ExtractionTuple

case class RegWrapper(
  val reg: ExtractionGroup[ReVerbExtraction]) extends ExtractionTuple {

  def arg1Norm = reg.arg1.norm
  def arg1EntityId = reg.arg1.entity.map(_.fbid)
  def arg1Types = reg.arg1.types.map(_.name)

  def relNorm = reg.rel.norm

  def arg2Norm = reg.arg2.norm
  def arg2EntityId = reg.arg2.entity.map(_.fbid)
  def arg2Types = reg.arg2.types.map(_.name)

  def setArg1Types(types: Set[String]) = RegWrapper(reg.copy(arg1 = reg.arg1.copy(types = types flatMap FreeBaseType.parse)))
  def setArg2Types(types: Set[String]) = RegWrapper(reg.copy(arg1 = reg.arg2.copy(types = types flatMap FreeBaseType.parse)))
}

object RegWrapper extends StringSerializer[RegWrapper] {

  override def serializeToString(wrapper: RegWrapper): String = ReVerbExtractionGroup.serializeToString(wrapper.reg)
  override def deserializeFromString(string: String): Option[RegWrapper] = ReVerbExtractionGroup.deserializeFromString(string) map RegWrapper.apply
}