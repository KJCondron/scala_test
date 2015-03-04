package concordance

import scala.collection.GenIterable

object Simple {
  def removePunct( t : String ) = t.replace(',', ' ')
  def findWords( sentence : String ) =
    removePunct(sentence).split(" ").map( _.trim.toLowerCase ).filterNot(_.size==0).toList
    
  def findSentences( text : String ) = {
    text.replace("i.e.", "_ID_EST_").split('.').map(
        _.replace("_ID_EST_", "i.e.")).toList
  }
  
  def time[A]( fn : =>A ) : (A,Double) =
  {
    val st=java.lang.System.nanoTime
    val ret = fn
    val ed=java.lang.System.nanoTime
    ( ret, (ed-st) * 1e-9 )
  }
  
  def check[T](o1 : T, o2 : T, msg : String) =
  {
    val ck = if(o1!=o2)
      msg + " don't" else msg
      
    println(ck + " match")
  }
  
  val EOS = "[EOS]"
    
  def findSentencesAndEOS( text : String ) = {
    val txt = if( text.length > 0 && text.last == '.')
      text.init + EOS else
      text
    
    val lines = findSentences(txt)
    
    // add EOS to all except last item
    (lines.init.map( _+EOS ) :+ lines.last).toIterable
  }
  
  def foldLines( lines : GenIterable[String] ) =
  {
    val ret = lines.foldLeft( List[String]() )(( acc,x ) =>
      acc match {
        case Nil => List(x)
        case _ => if(acc.head.contains(EOS))
        	x :: acc.head.replace(EOS,"") :: acc.tail else
        	acc.head.concat(" ").concat(x) :: acc.tail 
      })
      
      (ret.head.replace(EOS, "") :: ret.tail).reverse 
  }
  
  def printCC(cc : ConcordBasic.Concord) = {
  val words = cc.keys.toList.sorted
    
    words.foreach( w =>  { 
            print(w) 
            print("-")
            println( cc(w) )
        })
  }
}

object ConcordBasic {
  type Concord = Map[String, (Int, List[Int])]
  
  val empty = Map[String, (Int, List[Int])]()
  val sentenceNum = 0
  
  def buildConcordance(
      text : Iterable[String]) :
      Concord = 
  {
    val txt = text.mkString(" ")
    val sentences = Simple.findSentences(txt)
    val (concord, lineCount) =
      sentences.foldLeft( (empty, sentenceNum) )( (acc, sentence) => { 
        val newConcord = Simple.findWords(sentence).foldLeft(acc._1)( (acc2,word) => { 
          val(count, posns) = if(acc2.contains(word))
        	  					acc2(word)
        	  				else
        	  					(0,List[Int]())
          val posList = if( posns == Nil || acc._2 != posns.head ) acc._2 :: posns else posns // outer accumulator hold line number, don't add word that appears twice in a sentence
          acc2.updated( word, (count+1, posList) )
        })
      (newConcord, acc._2+1) })
    concord
  }
  
  def buildConcordance2(
      text : Iterable[String]) :
      Concord = 
  {
    val txt = text.mkString(" ")
    val sentences = Simple.findSentences(txt)
    val (concord, lineCount) =
      sentences.foldLeft( (empty, sentenceNum) )( (acc, sentence) => { 
        val wordsAndCounts = Simple.findWords(sentence).groupBy(x=>x).map(
            { case(x,y) => (x,y.size) } )
        val newConcord = wordsAndCounts.foldLeft(acc._1)( (cc,e) => { 
          val(count, posList) = if(cc.contains(e._1))
        	  					cc(e._1)
        	  				else
        	  					(0,List[Int]())
          cc.updated( e._1, (count+e._2, acc._2 :: posList) )
        })
      (newConcord, acc._2+1) })
    concord
  }
  
} // ConcordBasic

object Concord {
  type SentenceInfo = (Iterable[(String,Int)], Int)
  type Concord = Map[String, (Int, List[Int])]
  val empty = Map[String, (Int, List[Int])]()
  
  
  def process1Sentence
  (findWords  : String => Iterable[String])
  (sentence : String) : 
  Iterable[(String,Int)] = {
    findWords(sentence).groupBy(x=>x).map {
      case(word, wordList) => (word, wordList.size)
    }
  }
  
  def buildConcordance1
  ( findSentences : String => Iterable[String] )
  ( findWords : String => Iterable[String] )
  ( text : Iterable[String], par : Boolean = true ) =
    buildConcordance(findSentences)(findWords)(List(text.mkString(" ")), par)
    
  def buildConcordance
  ( findSentences : String => Iterable[String] )
  ( findWords : String => Iterable[String] )
  ( text : Iterable[String], par : Boolean = true ) : 
  Concord = {
    val sentences = if(par)
      text.par.flatMap( findSentences )
    else
      text.flatMap( findSentences )
      
    val sentenceHandler = process1Sentence(findWords) _
    val wordsAndCounts = sentences.map(sentenceHandler)
    
    val sentenceInfos = 
      wordsAndCounts.zipWithIndex
      
    updatedConcord(empty)(sentenceInfos)
  }
  
  def buildConcordance2
  ( findSentences : String => Iterable[String] )
  ( findWords : String => Iterable[String] )
  ( text : Iterable[String], par : Boolean = true ) : 
  Concord = {
    val textEOS = if(par)
      text.par.flatMap( findSentences )
    else
      text.flatMap( findSentences )
      
    val (sentences, t) = Simple.time(Simple.foldLines(textEOS))
    println("foldS: " + t)
    val sentenceHandler = process1Sentence(findWords) _
    val wordsAndCounts = sentences.map(sentenceHandler)
    
    val sentenceInfos = 
      wordsAndCounts.zipWithIndex
      
    updatedConcord(empty)(sentenceInfos)
  }
  
  def updatedConcord
  (current : Concord)
  (infos : GenIterable[SentenceInfo]) =
    infos.foldLeft(current)((acc,line) => 
      {
        val (words,lineNum) = line
        words.foldLeft(acc)((cc,word) => {
          val(count, posnList) = if(cc.contains(word._1)) cc(word._1) else (0,List[Int]())
          cc.updated(word._1, (count + word._2, lineNum :: posnList))
        })
      })
       
} // Concord

import Simple._

object testApp {

  def main(args: Array[String]): Unit = {
    
    val input = """Given an arbitrary text document written in English, write a program that will generate a concordance, i.e. an alphabetical list of all word occurrences, labeled with word frequencies. Bonus: label each word with the sentence numbers in which each occurrence appeared."""
    
    val cc1 = ConcordBasic.buildConcordance(List(input))
    val cc2 = ConcordBasic.buildConcordance2(List(input))
    
    val sh = Simple.findSentences _
    val wh = Simple.findWords _
    val shEOS = Simple.findSentencesAndEOS _
     
    def lm = io.Source.fromFile("""C:\Users\Karl\Documents\GitHub\concordance\data\lesMis.txt""").getLines.toIterable
    
    val (basic1, tBasic1) = time(ConcordBasic.buildConcordance(lm))
    val (basic2, tBasic2) = time(ConcordBasic.buildConcordance2(lm))
    
    println("basic1 took: " + tBasic1)
    println("basic2 took: " + tBasic2)
    
    check(basic1, basic2, "basic")
    check(basic1.keys, basic2.keys, "basic keys")
    
    val (mrst_mk, t_mrst_mk) = time(Concord.buildConcordance1(sh)(wh)(lm,false))
    val (mrmt_mk, t_mrmt_mk) = time(Concord.buildConcordance1(sh)(wh)(lm,true))
    
    check(basic1, mrst_mk, "basic / mk")
    check(mrmt_mk, mrst_mk, "mkmt / mkst")
    
    val (mrst1, t_mrst1) = time(Concord.buildConcordance(sh)(wh)(lm,false))
    val (mrmt1, t_mrmt1) = time(Concord.buildConcordance(sh)(wh)(lm,true))
    
    println("mrst1 took: " + t_mrst1)
    println("mrmt1 took: " + t_mrmt1)
    
    val (mrst2, t_mrst2) = time(Concord.buildConcordance2(sh)(wh)(lm,false))
    val (mrmt2, t_mrmt2) = time(Concord.buildConcordance2(sh)(wh)(lm,true))
    
    check(mrmt1, mrmt2, "mrmt 1 vs 2 shouldn't match and --->")
    check(basic1, mrmt2, "mrmt / basic")
    
    println("mrst2 took: " + t_mrst2)
    println("mrmt2 took: " + t_mrmt2)
    
    println("mrst_mk took: " + t_mrst_mk)
    println("mrmt_mk took: " + t_mrmt_mk)
    
    
  }
}