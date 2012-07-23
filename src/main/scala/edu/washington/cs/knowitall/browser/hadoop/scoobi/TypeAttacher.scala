package edu.washington.cs.knowitall.browser.hadoop.scoobi

import com.nicta.scoobi.Scoobi._
import UnlinkableEntityTyper.REG
import scopt.OptionParser

object TypeAttacher extends ScoobiApp {

  def run() = {
    
    var regsPath = ""
    var argTypesPath = ""
    var outRegsPath = ""
      
    val parser = new OptionParser() {
      arg("regsPath", "path to ReVerbExtractionGroups to which to attach types", { str => regsPath = str })
      arg("argTypesPath", "path to arg type predictions from UETyper", { str => argTypesPath = str })
      arg("outregsPath", "output path for ReVerbExtractionGroups with type predictions attached", { str => outRegsPath = str })
    }
    
    if (!parser.parse(args)) throw new IllegalArgumentException("Couldn't parse args")
    
    // now we should define a type prediction case class that encapsulates the data being transmitted between UETyper and us.
    // Currently, this data is being constructed ad-hoc at the end of UETyper.
    
  }
  
}