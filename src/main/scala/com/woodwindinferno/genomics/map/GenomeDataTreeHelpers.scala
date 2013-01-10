package com.woodwindinferno.genomics.map

import org.neo4j.graphdb.RelationshipType
import java.math.BigInteger
import org.neo4j.graphdb.Path
import org.neo4j.graphdb.traversal.Evaluator
import org.neo4j.graphdb.traversal.Evaluators
import org.neo4j.graphdb.traversal.Evaluation
import scala.math._



  object RelTypes extends Enumeration {  
    type RelTypes = Value
    val ALL_NODES_BOTH, ALL_NODES_BY_START, ALL_NODES_BY_END, ALL_NODES_BY_LENGTH, 
    LEFT, RIGHT, PARENT, DB_MD, 
    INTERVAL_TREE_ROOT, TEMP_S_LEFT_BY_START, TEMP_S_LEFT_BY_END, 
    TEMP_S_MID_BY_START, TEMP_S_MID_BY_END, TEMP_S_RIGHT_BY_START, TEMP_S_RIGHT_BY_END,
    NODES_BY_START, NODES_BY_END, QUERY_RESULT, TRASH= Value

    implicit def conv(rt: RelTypes) = new RelationshipType() {def name = rt.toString}
  }

    
  object Colors extends Enumeration {
      type Colors = Value
      val RED, BLACK = Value    
      
      implicit def conv(pk: Colors):Object = pk.toString()
      implicit def asColor(s:String):Colors = {
        s match {
          case "RED" => RED
          case _ => BLACK
        }
      }
    }

  object DataConstants {
    val SIZE_OF_BASE_IN_BITS:Int = 64
    val SIZE_OF_CHROMOSOME_NUMBER_IN_BITS:Int = 12
    val Y_CHROM_NUMBER:Int = pow(2.0, SIZE_OF_CHROMOSOME_NUMBER_IN_BITS.doubleValue()).intValue() + 2
    val X_CHROM_NUMBER:Int = pow(2.0, SIZE_OF_CHROMOSOME_NUMBER_IN_BITS.doubleValue()).intValue() + 1
  }
  
  object PropKeys extends Enumeration {
    type PropKeys = Value
    val KEY, VALUE, COLOR = Value    
  
    implicit def conv(pk: PropKeys) = pk.toString()
  }
  
  object IntervalTreePropKeys extends Enumeration {
    type PropKeys = Value
    val MID, MAX_LENGTH = Value    
    
    implicit def conv(pk: PropKeys) = pk.toString()
  }
  
    
  case  class OrderedBigIntWrapper(val wrapped:String) extends Ordered[String] {
    lazy val renderedVal:BigInt = BigInt(wrapped)
    override def compare(that: String): Int = renderedVal.compare(BigInt(that))
  }
  
  case class DeDupingQueryResultWrapper(val key:String) extends Ordered[String] {
    val valPair:Tuple2[BigDecimal, Long] =  parse(key)
    
    private def parse(s:String):(BigDecimal, Long) = { 
      var valArr:Array[String] = s.split("_")
      (BigDecimal(valArr(0)), valArr(1).toLong)
    }
    
    override def compare(that: String): Int = {
      val thatInfo = parse(that)
      val mRet = this.valPair._1.compare(thatInfo._1)
      if((mRet) != 0) mRet
      else this.valPair._2.compare(thatInfo._2)
    }
  }

  object RBTravEvaluator extends Evaluator {
    override def evaluate(p:Path):Evaluation = {
      if(!p.endNode().hasRelationship(RelTypes.LEFT)) Evaluation.INCLUDE_AND_PRUNE
      else Evaluation.EXCLUDE_AND_CONTINUE
    }
  }
 
