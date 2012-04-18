package edu.washington.cs.knowitall.browser.hadoop.scoobi

  import com.nicta.scoobi.Scoobi._
  import com.nicta.scoobi.DList._
  import com.nicta.scoobi.io.text.TextInput._
  import com.nicta.scoobi.io.text.TextOutput._

class FbidLinkerTest {

  def main(allArgs: Array[String]) = withHadoopArgs(allArgs) { args =>
  
    val lines = fromTextFile(args(0))
  
    val counts = lines.flatMap(_.split(" ")).map(word => (word, 1)).groupByKey.combine[String, Int](_+_)

    persist(toTextFile(counts, args(1)))
  }
  
}