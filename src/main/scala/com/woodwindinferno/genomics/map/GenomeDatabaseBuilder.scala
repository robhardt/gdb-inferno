package com.woodwindinferno.genomics.map

import scala.collection.Iterable
import org.neo4j.graphdb.Node
import org.neo4j.scala.Neo4jWrapper
import org.neo4j.graphdb.index.IndexManager
import org.neo4j.scala.EmbeddedGraphDatabaseServiceProvider
import scala.io.Source
import org.neo4j.scala.DatabaseService
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.tooling.GlobalGraphOperations
import scala.collection.JavaConverters._
import org.neo4j.graphdb.TraversalPosition
import org.neo4j.graphdb.Direction
import org.neo4j.graphdb.Relationship
import org.neo4j.graphdb.RelationshipType
import java.math.BigInteger
import RelTypes._
import org.neo4j.graphdb.Transaction
import org.neo4j.scala.BatchGraphDatabaseServiceProvider
import org.neo4j.kernel.EmbeddedGraphDatabase
import scala.actors.threadpool.Executors
import scala.actors.threadpool.ExecutorService
import java.util.Date
import org.neo4j.graphdb.traversal.TraversalDescription
import org.neo4j.graphdb.traversal.Evaluators
import org.neo4j.kernel.Traversal
import org.neo4j.graphdb.traversal.Traverser
import org.neo4j.graphdb.Path
import org.neo4j.graphdb.traversal.Evaluator
import org.neo4j.graphdb.traversal.Evaluators
import org.neo4j.kernel.Uniqueness
import scala.collection.JavaConverters



/**
 * This takes an input file in the format "chromosome  start  end  value array" and creates an on-disk neo4j database that can be efficiently queried
 * this runs best with at least 4GB of RAM and preferably >8GB.  It should work with less memory, but neo4j starts requiring frequent, expensive 
 * garbage collections after about 300k rows of data.
 */
class GenomeDatabaseBuilder(val mode:String, val inputFile:String, val dbLoc:String, val createNew:Boolean=false, val opsPerTx:Int = 500) extends Neo4jWrapper with EmbeddedGraphDatabaseServiceProvider {
 
  def neo4jStoreDir = dbLoc
  
  
  //optimized for batch insert
  override def configParams = Map[String, String](
      "keep_logical_logs" -> "false",
      "neostore.nodestore.db.mapped_memory" -> "90M",
      "neostore.relationshipstore.db.mapped_memory" -> "3G",
      "neostore.propertystore.db.mapped_memory" -> "50M",
      "neostore.propertystore.db.strings.mapped_memory" -> "100M",
      "neostore.propertystore.db.arrays.mapped_memory" -> "0M"
  )
  
  
  //convenience function to allow for operations to occur within a transaction
  override def withTx[T <: Any](operation: DatabaseService => T): T = {
    val tx = synchronized {
      ds.gds.beginTx()
    }
    try {
      val ret = operation(ds)
      tx.success
      return ret
    } finally {
      tx.finish
    }
  }
  
  def wipeDb = {
    //for now, we're going to assume an empty db and disable this.  Just too dangerous
//    withTx{
//      ds =>
//        GlobalGraphOperations.at(ds.gds).getAllNodes().asScala.map(_.getRelationships.asScala.map(_.delete));// asScala().map(_.getRelationships().asScala.map(_.delete))
//        GlobalGraphOperations.at(ds.gds).getAllNodes().asScala.filter(_.getId() != 0).map(_.delete);
//    }
  }
   
  def buildDB:Unit = {
      
    registerShutdownHook
    
    if(createNew){
      //ignoring for now.  Too dangerous
      wipeDb
      withTx{
      ds =>
        Neo4jRedBlackNode.createRoot(ds.gds, RelTypes.ALL_NODES_BOTH)
        
        Neo4jRedBlackNode.createRoot(ds.gds, RelTypes.DB_MD)
      
      }
    } 
    
    if(mode.equals("show")) show
    else if(mode.equals("build")) theLastBuildYoullEverNeed
    else if(mode.equals("clean")) clean
    else println("Unknown mode: "+this.mode)

  }
  
  def clean = {
    withTx{
      ds =>
        ds.gds.getReferenceNode().getRelationships(RelTypes.QUERY_RESULT, Direction.OUTGOING).asScala.foreach{
          r:Relationship =>
            val n:Node = r.getEndNode()
            r.delete
            if(n != null){
              Neo4jRedBlackNode[String, Long](n).traverseForDelete.foreach{
                n:Node =>
                  n.getRelationships().asScala.foreach(_.delete)
                  n.delete
              }
            }
        }
    }
  }
  
   /**
    * actually builds the database.  Iterates through the file, converts each row into a Scala object, 
    * associates that object with a Neo4j node and inserts it.
    * It this puts 2 entries into a red-black tree imlpemented in Neo4j
    * one for the row's start value and one for the end value
    */
   def theLastBuildYoullEverNeed = {
        val lines:Iterator[String] = Source.fromFile(inputFile).getLines
        //val numLines:Int = lines.size    
        
        val lineIter:Iterator[(Int, String)] = (0 to Int.MaxValue - 1).iterator zip lines

        var tx:Transaction = ds.gds.asInstanceOf[EmbeddedGraphDatabase].tx().unforced().begin()
        try{
          val bothRoot = Neo4jRedBlackNode.getRoot[String, Long](ds.gds, RelTypes.ALL_NODES_BOTH)(s => OrderedBigIntWrapper(s))
          val insertResults = lineIter.foldLeft((BigInt(0),bothRoot)){
            (accumTuple, lineTuple) =>
              if((lineTuple._1 != 0) && (lineTuple._1 % opsPerTx == 0)){
                println("BOTH - "+new Date() + " - " +lineTuple._1)
                tx.success
                tx.finish
                tx = ds.gds.beginTx()
              }
              val genomeDataForLine:GenomeDataNode = GenomeDataNode(lineTuple._2)
              val dataNode:Node = createNode(genomeDataForLine)(ds)
              val retNode = accumTuple._2.insert(ds.gds, genomeDataForLine.derivedStart, dataNode.getId())(s => OrderedBigIntWrapper(s)).
                                insert(ds.gds, genomeDataForLine.derivedEnd, dataNode.getId())(s => OrderedBigIntWrapper(s))
              val maxLen = accumTuple._1.max(BigInt(genomeDataForLine.length))
              //println(genomeDataForLine.chromosome)
              //println(genomeDataForLine.start)
              //println(genomeDataForLine.derivedStart)
              (maxLen, retNode)
          }
          
          val mdRoot = Neo4jRedBlackNode.getRoot[String, Long](ds.gds, RelTypes.DB_MD)
          mdRoot.node.setProperty(IntervalTreePropKeys.MAX_LENGTH, insertResults._1.toString)
          
        }
        finally{
          if(tx != null){
          tx.success
          tx.finish            
          }
        }
  } 
   
   /**
    * show some diagnostic data - i.e. whether a database build was complete (result = 2 * number of lines in the input file)
    */
   def show = {
     withTx{
       db =>
         val sr:Neo4jRedBlackNode[String, Long] = Neo4jRedBlackNode.getRoot[String, Long](ds.gds, RelTypes.ALL_NODES_BOTH)(s => OrderedBigIntWrapper(s))
         //val er:Neo4jRedBlackNode[String, Long] = Neo4jRedBlackNode.getRoot[String, Long](ds.gds, RelTypes.ALL_NODES_BY_END)(s => OrderedBigIntWrapper(s))
         
         
         
         val startCountNormalTrav = sr.traverse.foldLeft(0){
           (accum:Int, next:Node) =>
             accum + 1
         }
         println(startCountNormalTrav)
     }
     
     
   }
   
  /**
   * attempt to properly close database on app shutdown.
   */
  def registerShutdownHook = Runtime.getRuntime.addShutdownHook(new Thread() { override def run = shutdown })

  private def shutdown = {
    ds.gds.shutdown
  }
  
}


object GenomeDatabaseBuilder extends App {

    if(args.length==0 || args(0) == null || args(0).trim.isEmpty || args(0).equals("GUI")){
      GenomeDbGUI.main(Array[String]())
    }
    else if(args(0).equals("query")){
      val querier:GenomeDatabaseQuerier = new GenomeDatabaseQuerier(args(1), args(2), args(3).toBoolean)
      val resultNodes:(GenomeDataNode, GenomeDataNode) = parseQuery(args(4))
      querier.queryDb(null, resultNodes._1, resultNodes._2, args(5).toBoolean, args(6).toBoolean).foreach{
        gdn =>
          println(gdn.toString)
      }
    }
    else{
      val builder:GenomeDatabaseBuilder ={
        if(args.length == 3) new GenomeDatabaseBuilder(args(0), args(1), args(2))
        else if(args.length == 4) new GenomeDatabaseBuilder(args(0), args(1), args(2), args(3).toBoolean)
        else if(args.length == 5) new GenomeDatabaseBuilder(args(0), args(1), args(2), args(3).toBoolean, args(4).toInt)
        else null
      }
      builder.buildDB
    }
    
    
    /**
     * get the left-most entry in the red-black tree (smallest key)
     */
    def getMin(root:Neo4jRedBlackNode[String, Long]):String = {
      root.left match {
        case LEAF(node) => root.key
        case TREE(node) => getMin(root.left)
      }
    }
  
    
    /**
     * get the right-most entry in the red-black tree (largest key)
     */
    def getMax(root:Neo4jRedBlackNode[String, Long]):String = {
      root.right match {
        case LEAF(node) => root.key
        case TREE(node) => getMax(root.right)
      }
    }
    
    /**
     * takes the user's query and converts it into 2 GenomeDataNode objects - 
     * one representing the start of the range, and one representing the end
     */
    def parseQuery(query:String) = {
      val halves:List[Array[String]] = query.split("-").toList.map(_.split(":"))
      val leftChr:String = halves.head(0);
      val leftStart:String = halves.head(1);
      val hasRightChr:Boolean = (halves.tail.head.length == 2)
      val rightChr = if(hasRightChr) halves.tail.head(0) else leftChr
      val rightEnd = if(hasRightChr) halves.tail.head(1) else halves.tail.head(0)
      
      (GenomeDataNode(leftChr, BigInt(leftStart), BigInt(leftStart), 0, 0), GenomeDataNode(rightChr, BigInt(rightEnd), BigInt(rightEnd), 0, 0))
      
    }
    

}











/*


def buildIntervalTree(treeRootNodesByStart:Neo4jRedBlackNode[String, Long], treeRootNodesByEnd:Neo4jRedBlackNode[String, Long]):Node = {
    println("build interval tree")
    treeRootNodesByStart match {
      // if the list is empty, return nothing
      case LEAF(node) => withTx[Node]{ds => createNode(ds)}
      case TREE(xNode) => {
        
        //create temp trees
        Neo4jRedBlackNode.createRoot(ds.gds, RelTypes.TEMP_S_LEFT_BY_START)
        Neo4jRedBlackNode.createRoot(ds.gds, RelTypes.TEMP_S_LEFT_BY_END)
        Neo4jRedBlackNode.createRoot(ds.gds, RelTypes.TEMP_S_MID_BY_START)
        Neo4jRedBlackNode.createRoot(ds.gds, RelTypes.TEMP_S_MID_BY_END)
        Neo4jRedBlackNode.createRoot(ds.gds, RelTypes.TEMP_S_RIGHT_BY_START)
        Neo4jRedBlackNode.createRoot(ds.gds, RelTypes.TEMP_S_RIGHT_BY_END)
        
        
        //find the smallest start and the biggest end, and find the middle
        val minStart:BigInt = BigInt(getMin(treeRootNodesByStart))
        val maxEnd:BigInt = BigInt(getMax(treeRootNodesByEnd))
        
        
        val mid:BigInt = ((maxEnd - minStart) / 2) + minStart
        
        treeRootNodesByEnd.traverseSubset(minStart.toString, mid.toString).foreach{
          n:Node => 
            val rbn:Neo4jRedBlackNode[String, Long] = TREE[String, Long](n)
            val dataNode:GenomeDataNode = getNodeById(rbn.value)(ds).toCC[GenomeDataNode].get
            if(BigInt(dataNode.derivedEnd) >= mid){
              Neo4jRedBlackNode.getRoot[String, Long](ds.gds, RelTypes.TEMP_S_MID_BY_START).insert(ds.gds, dataNode.derivedStart, rbn.value)
              Neo4jRedBlackNode.getRoot[String, Long](ds.gds, RelTypes.TEMP_S_MID_BY_END).insert(ds.gds, dataNode.derivedEnd, rbn.value)
            }
            else{
              Neo4jRedBlackNode.getRoot[String, Long](ds.gds, RelTypes.TEMP_S_LEFT_BY_START).insert(ds.gds, dataNode.derivedStart, rbn.value)
              Neo4jRedBlackNode.getRoot[String, Long](ds.gds, RelTypes.TEMP_S_LEFT_BY_END).insert(ds.gds, dataNode.derivedEnd, rbn.value)
            }
        }
        
        treeRootNodesByEnd.traverseSubset(mid.toString, maxEnd.toString).foreach{
          n:Node =>
            val rbn:Neo4jRedBlackNode[String, Long] = TREE[String, Long](n)
            val dataNode:GenomeDataNode = getNodeById(rbn.value)(ds).toCC[GenomeDataNode].get
            Neo4jRedBlackNode.getRoot[String, Long](ds.gds, RelTypes.TEMP_S_RIGHT_BY_START).insert(ds.gds, dataNode.derivedStart, rbn.value)
            Neo4jRedBlackNode.getRoot[String, Long](ds.gds, RelTypes.TEMP_S_RIGHT_BY_END).insert(ds.gds, dataNode.derivedEnd, rbn.value)
        }

        val treeNodes:Array[Neo4jRedBlackNode[String, Long]] = withTx[Array[Neo4jRedBlackNode[String, Long]]]{
          ds =>
            val leftByStart:Neo4jRedBlackNode[String, Long] = Neo4jRedBlackNode.getRoot[String, Long](ds.gds, RelTypes.TEMP_S_LEFT_BY_START)
            val leftByEnd:Neo4jRedBlackNode[String, Long] = Neo4jRedBlackNode.getRoot[String, Long](ds.gds, RelTypes.TEMP_S_LEFT_BY_END)
            val midByStart:Neo4jRedBlackNode[String, Long] = Neo4jRedBlackNode.getRoot[String, Long](ds.gds, RelTypes.TEMP_S_MID_BY_START)
            val midByEnd:Neo4jRedBlackNode[String, Long] = Neo4jRedBlackNode.getRoot[String, Long](ds.gds, RelTypes.TEMP_S_MID_BY_END)
            val rightByStart:Neo4jRedBlackNode[String, Long] = Neo4jRedBlackNode.getRoot[String, Long](ds.gds, RelTypes.TEMP_S_RIGHT_BY_START)
            val rightByEnd:Neo4jRedBlackNode[String, Long] = Neo4jRedBlackNode.getRoot[String, Long](ds.gds, RelTypes.TEMP_S_RIGHT_BY_END)
            
            leftByStart.node.getSingleRelationship(RelTypes.TEMP_S_LEFT_BY_START, Direction.INCOMING).delete
            leftByEnd.node.getSingleRelationship(RelTypes.TEMP_S_LEFT_BY_END, Direction.INCOMING).delete
            midByStart.node.getSingleRelationship(RelTypes.TEMP_S_MID_BY_START, Direction.INCOMING).delete
            midByEnd.node.getSingleRelationship(RelTypes.TEMP_S_MID_BY_END, Direction.INCOMING).delete
            rightByStart.node.getSingleRelationship(RelTypes.TEMP_S_RIGHT_BY_START, Direction.INCOMING).delete
            rightByEnd.node.getSingleRelationship(RelTypes.TEMP_S_RIGHT_BY_END, Direction.INCOMING).delete

            (leftByStart :: leftByEnd :: midByStart :: midByEnd :: rightByStart :: rightByEnd :: Nil).toArray[Neo4jRedBlackNode[String, Long]]
        }
        
        //now we can get rid of the source trees
        withTx{
          ds =>
            treeRootNodesByStart.traverseForDelete.map{
              n:Node =>
                n.getRelationships().asScala.map(_.delete)
                n.delete
            }
        }
        
        //now we can get rid of the source trees
        withTx{
          ds =>
            treeRootNodesByEnd.traverseForDelete.map{
              n:Node =>
                n.getRelationships().asScala.map(_.delete)
                n.delete
            }
        }
        
        val ret:Node = withTx[Node]{
          ds =>
            val ret2 = createNode(ds)
            ret2.setProperty(IntervalTreePropKeys.MID, mid.toString)
            ret2.createRelationshipTo(treeNodes(2).node, NODES_BY_START)
            ret2.createRelationshipTo(treeNodes(3).node, NODES_BY_END)
            ret2
        }

        val leftStartIntervalTreeRoot:Node = buildIntervalTree(treeNodes(0), treeNodes(1)) 
        val rightStartIntervalTreeRoot:Node = buildIntervalTree(treeNodes(4), treeNodes(5))
        
        withTx{
          ds =>         
            ret.createRelationshipTo(leftStartIntervalTreeRoot, LEFT)
            ret.createRelationshipTo(rightStartIntervalTreeRoot, RIGHT)
        }
        ret
      }
    }
    
    
 */ 




