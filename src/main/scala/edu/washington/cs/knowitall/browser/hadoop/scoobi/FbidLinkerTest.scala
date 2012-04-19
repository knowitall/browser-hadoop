package edu.washington.cs.knowitall.browser.hadoop.scoobi

  import com.nicta.scoobi.Scoobi._
  import com.nicta.scoobi.DList._
  import com.nicta.scoobi.DList
  import com.nicta.scoobi.io.text.TextInput._
  import com.nicta.scoobi.io.text.TextInput
  import com.nicta.scoobi.io.text.TextOutput._
  import com.nicta.scoobi.io.text.TextOutput
  
  import java.io.File
  import java.io.FileWriter
  
  import scala.collection.JavaConversions._
  
  import edu.washington.cs.knowitall.browser.hadoop.entity.OneTranslation
  
object FbidLinkerTest {

  def listWrapper(inList : java.util.List[String]): Iterable[String] = {
    
    inList.toIterable
  }
  
  def main(args: Array[String]) = withHadoopArgs(args) { a =>

    val trans = new OneTranslation()
    
    val (inputPath, outputPath) = (a(0), a(1))

    val lines: DList[String] = TextInput.fromTextFile(inputPath)

    // map 
    val keyValuePair: DList[(String, Iterable[String])] = lines.flatMap { line => 
      
      line.split("\t") match {
        case Array(sIdx, arg1, rel, arg2, _*) => Some(arg1, listWrapper(trans.linkToFbids(arg1)))
        case _ => None
      }
      
    }

    //val combined: DList[(String, Int)] = grouped.combine((_+_))

    DList.persist(TextOutput.toTextFile(keyValuePair, outputPath + "/test-results"));
  }  
}