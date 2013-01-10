package com.woodwindinferno.genomics.map

import org.neo4j.scala.Neo4jWrapper
import org.neo4j.graphdb.Node
import org.neo4j.scala.DatabaseService
import DataConstants._
import RelTypes._
import org.neo4j.scala.EmbeddedGraphDatabaseServiceProvider



/**
 * represents a line of data in the input file.
 * it captures all the data as well as some derived numbers based on chromosome + start, and chromosome + end
 * it also precalculates the range length, which is needed for the out-lier algo
 */
case class GenomeDataNode(val chromosome:String, val start:String, val end:String, 
    val value:String, val array:Int, val derivedStart:String, val derivedEnd:String, val length:String) extends Ordered[GenomeDataNode]{
  
  override def compare(that:GenomeDataNode):Int = {
    val c1 = BigDecimal(value).compare(BigDecimal(that.value))
    if(c1 != 0) c1
    else{
      val c2 = BigInt(start).compare(BigInt(that.start))
      if(c2 != 0) c2
      else BigInt(end).compare(BigInt(that.end))
    }
  }
  
  def equals(that:GenomeDataNode) = {
    compare(that) == 0
  }
  
  override def toString = chromosome+"\t"+start+"\t"+end+"\t"+value+"\t"+array
  
}
    
object GenomeDataNode{
  def apply(chromosome:String, start:BigInt, end:BigInt, value:BigDecimal, array:Int):GenomeDataNode = {
      val chromosomeDerivedNumber:BigInt = {
        if(chromosome.matches("^.+[xX]$")) X_CHROM_NUMBER
        else if(chromosome.matches("^.+[yY]$")) Y_CHROM_NUMBER
        else BigInt(chromosome.foldLeft(List[Char]())((runningList, nextChar) => {
          //basically run through the string, when we hit a digit, add it to the list, if we hit another non-digit after that, empty the list and start over
          //should convert zzzzzzz34 => 34
          //and zzz12345zzz678 => 678
          //if the string doesn't end in digits, let BigInt throw the error
          if(!nextChar.isDigit){
            if(runningList.isEmpty) runningList
            else List[Char]()
          }
          else{
            nextChar :: runningList
          }
        }).reverse.mkString)
      } 
  
    // derived numbers that allow us to do numeric comparisons across multiple chromosomes - BigInt is probably overkill, but space is cheap
    val derivedStart:BigInt = BigInt(chromosomeDerivedNumber.bigInteger.shiftLeft(SIZE_OF_BASE_IN_BITS).add(start.bigInteger).toString())
    val derivedEnd:BigInt = BigInt(chromosomeDerivedNumber.bigInteger.shiftLeft(SIZE_OF_BASE_IN_BITS).add(end.bigInteger).toString())
    //val derivedCenter:BigInt = derivedStart + ((derivedEnd - derivedStart) / 2)
    val length:BigInt = derivedEnd - derivedStart
    GenomeDataNode(chromosome, start.toString, end.toString, value.toString, array, derivedStart.toString, derivedEnd.toString, length.toString)
  }
  def apply(lineFromFile:String):GenomeDataNode = {
    val lineArr:Array[String] = lineFromFile.split("\\s+")
    GenomeDataNode(lineArr(0), BigInt(lineArr(1)), BigInt(lineArr(2)), BigDecimal(lineArr(3)), lineArr(4).toInt)
  }
  
  def main(args:Array[String]) = {
    val poo = GenomeDataNode("chr20", BigInt("33637606"), BigInt("33637606"), BigDecimal("0.11414998024702072"), 0)
    println(poo)
    (1 to 22).iterator.foreach{
      i:Int =>
        println(i)
        val x = BigInt(i.toString).bigInteger.shiftLeft(SIZE_OF_BASE_IN_BITS)
        println(x)
        val chromosome = "chr"+i
        val chromosomeDerivedNumber:BigInt = {
        if(chromosome.matches("^.+[xX]$")) X_CHROM_NUMBER
        else if(chromosome.matches("^.+[yY]$")) Y_CHROM_NUMBER
        else BigInt(chromosome.foldLeft(List[Char]())((runningList, nextChar) => {
          //basically run through the string, when we hit a digit, add it to the list, if we hit another non-digit after that, empty the list and start over
          //should convert zzzzzzz34 => 34
          //and zzz12345zzz678 => 678
          //if the string doesn't end in digits, let BigInt throw the error
          if(!nextChar.isDigit){
            if(runningList.isEmpty) runningList
            else List[Char]()
          }
          else{
            nextChar :: runningList
          }
        }).reverse.mkString)
        }
        println(chromosome)
        println(chromosomeDerivedNumber)
        println(chromosomeDerivedNumber + BigInt("72796"))
    }
    ("X"::"Y"::Nil).iterator.foreach{
      i =>
        println(i)
        val chromosome = "chr"+i
        val chromosomeDerivedNumber:BigInt = {
        if(chromosome.matches("^.+[xX]$")) X_CHROM_NUMBER
        else if(chromosome.matches("^.+[yY]$")) Y_CHROM_NUMBER
        else BigInt(chromosome.foldLeft(List[Char]())((runningList, nextChar) => {
          //basically run through the string, when we hit a digit, add it to the list, if we hit another non-digit after that, empty the list and start over
          //should convert zzzzzzz34 => 34
          //and zzz12345zzz678 => 678
          //if the string doesn't end in digits, let BigInt throw the error
          if(!nextChar.isDigit){
            if(runningList.isEmpty) runningList
            else List[Char]()
          }
          else{
            nextChar :: runningList
          }
        }).mkString)
        }
        println(chromosome)
        println(chromosomeDerivedNumber)
        val x = BigInt(chromosomeDerivedNumber.toString).bigInteger.shiftLeft(SIZE_OF_BASE_IN_BITS)
        println(x)
        //println(("1" :: "0" :: Nil).mkString())
    }
    
  }
  
}


