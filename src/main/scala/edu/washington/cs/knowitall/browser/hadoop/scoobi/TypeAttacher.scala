package edu.washington.cs.knowitall.browser.hadoop.scoobi

import com.nicta.scoobi.Scoobi._
import com.nicta.scoobi.lib.Relational
import UnlinkableEntityTyper.REG
import scopt.OptionParser
import edu.washington.cs.knowitall.browser.extraction.ReVerbExtractionGroup
import edu.washington.cs.knowitall.browser.hadoop.scoobi.util.{ Arg1, Arg2, ArgField }
import edu.washington.cs.knowitall.browser.hadoop.scoobi.util.TypePrediction

object TypeAttacher extends ScoobiApp {

  def run() = {

    var regsPath = ""
    var argTypesPath = ""
    var outRegsPath = ""
    var argField: ArgField = null

    val parser = new OptionParser() {
      arg("regsPath", "path to ReVerbExtractionGroups to which to attach types", { str => regsPath = str })
      arg("argTypesPath", "path to arg type predictions from UETyper", { str => argTypesPath = str })
      arg("outregsPath", "output path for ReVerbExtractionGroups with type predictions attached", { str => outRegsPath = str })
      arg("arg", "arg1 to predict types for arg1's, arg2 to predict types for arg2s", { str =>
        if (str.equals("arg1")) argField = Arg1()
        else if (str.equals("arg2")) argField = Arg2()
        else throw new IllegalArgumentException("arg must be either arg1 or arg2")
      })
    }

    if (!parser.parse(args)) throw new IllegalArgumentException("Couldn't parse args")

    this.configuration.jobNameIs("Type-Prediction-Attacher".format(argField.name))
    
    def loadReg(str: String) = ReVerbExtractionGroup.fromTabDelimited(str.split("\t"))._1
    def argRegPair(reg: REG) = (argField.getArgNorm(reg), ReVerbExtractionGroup.toTabDelimited(reg))
    def argTypePair(typePred: TypePrediction) = (typePred.argString, typePred.toString)
    // first step is to do a join to match REGs with their Option[TypePrediction]
    val argRegPairs = fromTextFile(regsPath) flatMap loadReg map argRegPair
    val argTypePredPairs = fromTextFile(argTypesPath) flatMap TypePrediction.fromString map argTypePair

    val leftJoined = Relational.joinLeft(argRegPairs, argTypePredPairs)

    // now just attach types where we aren't already linked
    def tryAttachType(reg: REG, typePred: TypePrediction) = {
      def typeInts = typePred.predictedTypes.map(_._1.enum)
      if (argField.getTypeStrings(reg).isEmpty) argField.attachTypes(reg, typeInts) else reg
    }

    var numArgs = 0L
    var numAlreadyTyped = 0L
    var numTypesAttached = 0L

    val finalResult = leftJoined map {
      case (argString, (regString, optTypePred)) => {
        if (numArgs % 2000 == 0) System.err.println("NumArgs: %s, NumTypesAttached: %s, NumAlreadyTyped: %s".format(numArgs, numAlreadyTyped, numTypesAttached)) 
        numArgs += 1
        optTypePred match {
          case Some(typePredString) => {
            lazy val reg = ReVerbExtractionGroup.fromTabDelimited(regString.split("\t"))._1.get
            TypePrediction.fromString(typePredString) match {
              case Some(typePred) => {
                val newReg = tryAttachType(reg, typePred)
                if (argField.getTypeStrings(reg).isEmpty) numTypesAttached += 1 else numAlreadyTyped += 1
                if (argField.getTypeStrings(newReg).isEmpty) System.err.println("Strange: %s\t%s".format(typePredString, regString))
                ReVerbExtractionGroup.toTabDelimited(newReg)
              }
              case None => regString
            }
          }
          case None => regString
        }
      }
    }
    
    persist(toTextFile(finalResult, outRegsPath + "/"))
  }
}