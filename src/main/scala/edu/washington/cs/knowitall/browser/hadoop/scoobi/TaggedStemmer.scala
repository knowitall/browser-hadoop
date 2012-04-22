package edu.washington.cs.knowitall.browser.hadoop.scoobi

import java.util.concurrent.Semaphore
import java.io.StringReader
import uk.ac.susx.informatics.Morpha

/**
 * Copied and translated from a java implementation, which itself was originally adapted from other Java source
 */
object TaggedStemmer {
  val modifierTags = Set("JJ", "RB", "$PRP", "VBG")
  val determinerTags = Set("DT", "PDT", "WDT")
  val modifiersToKeepTemp = Set("n't", "not", "no", "as",
                "rarely", "hardly", "never", "most", "least", "few", "some",
                "many", "none", "ought", "would", "could", "should")
  val ignorableNounTags = Set("WP", "EX", "SYM")
  
  def getInstance = new TaggedStemmer(new Morpha(System.in))
}

class TaggedStemmer(val _lexer: Morpha) {

  // There must have been some slight difference between
  // two different POS tag sets:
    def mapTag(posTag: String): String = {
    	
        if (posTag.startsWith("NNP"))
            return "NP";
        
        return posTag;
    }


    def stemAll(pairs: Iterable[(String, String)]): Iterable[String] = {
      pairs.map(p=>stem(p._1, p._2))
    }

    def stem(word: String, oldTag: String): String = {

        var returnVal = ""
     
        val tag = mapTag(oldTag)
        val wordtag = word + "_" + tag
        var word_norm: String = ""
        try {
            _lexer.yyreset(new StringReader(wordtag));
            _lexer.yybegin(Morpha.scan);
            
            word_norm = _lexer.next()
            // String tag_norm= _lexer.next();
            returnVal = word_norm;
        } catch {
          case t: Throwable => returnVal = word
        } 
        
        if (returnVal == null) returnVal = word
        
        // Morpha doesn't properly singularize a plural proper noun.
        if (oldTag.equalsIgnoreCase("NNPS")) {
        	if (returnVal.endsWith("es") && returnVal.length() > 2) {
        		returnVal = returnVal.substring(0, returnVal.length()-2)
        	}
        	else if (returnVal.endsWith("s")) {
        		returnVal = returnVal.substring(0, returnVal.length()-1)
        	}
        }
        
        return returnVal;
    }

    def isPunct(tag: String): Boolean = {
        if (Character.isLetter(tag.charAt(0)))
            return false
        return true
    }
}
