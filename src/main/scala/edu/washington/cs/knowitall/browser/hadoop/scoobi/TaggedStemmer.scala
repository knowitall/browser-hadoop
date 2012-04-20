package edu.washington.cs.knowitall.browser.hadoop.scoobi

import java.util.concurrent.Semaphore
import java.io.StringReader
import uk.ac.susx.informatics.Morpha

object TaggedStemmer {
  val modifierTags = Set("JJ", "RB", "$PRP", "VBG")
  val determinerTags = Set("DT", "PDT", "WDT")
  val modifiersToKeepTemp = Set("n't", "not", "no", "as",
                "rarely", "hardly", "never", "most", "least", "few", "some",
                "many", "none", "ought", "would", "could", "should")
  val ignorableNounTags = Set("WP", "EX", "SYM")
}

class TaggedStemmer(val _lexer: Morpha) {

  // There must have been some slight difference between
  // two different POS tag sets:
    def mapTag(posTag: String): String = {
    	
        if (posTag.startsWith("NNP"))
            return "NP";
        
        return posTag;
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
        } catch (Throwable e) {
            returnVal = word;
        } 
        
        if (returnVal == null) returnVal = word;
        
        // Morpha doesn't properly singularize a plural proper noun.
        if (oldTag.equalsIgnoreCase("NNPS")) {
        	if (returnVal.endsWith("es") && returnVal.length() > 2) {
        		returnVal = returnVal.substring(0, returnVal.length()-2);
        	}
        	else if (returnVal.endsWith("s")) {
        		returnVal = returnVal.substring(0, returnVal.length()-1);
        	}
        }
        
        return returnVal;
    }

    private boolean isPunct(String tag) {
        if (Character.isLetter(tag.charAt(0)))
            return false;
        return true;
    }

    // normalizes, stripping only DT, ADJ, and ADV.
	public String[] stemReVerbRelation(List<String> tokens, List<String> postags) {
		
		String[] stemmed = new String[tokens.size()];
        for (int i = 0; i < tokens.size(); ++i) {
            String token = tokens.get(i).toLowerCase();
            String tag = postags.get(i);
            String temp = "";
            if (tag.equalsIgnoreCase("DT")) continue;
            else if (tag.startsWith("RB") || tag.startsWith("JJ")) {
            	temp = tag;
            }
            else {
            	temp = stem(token, tag, false);
            }
            stemmed[i] = temp.trim();
        }
        return stemmed;
	}

}
