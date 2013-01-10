package com.woodwindinferno.genomics.map

import org.neo4j.scala.Neo4jWrapper
import org.neo4j.scala.EmbeddedGraphDatabaseServiceProvider
import org.neo4j.graphdb.Direction
import scala.collection.JavaConverters._
import org.neo4j.graphdb.Node


/**
 * 
 * This handles actually traversing the database and sorting the results
 * 
 * @author rob
 *
 */
class GenomeDatabaseQuerier(val dbLoc:String, val outputFile:String, val printToConsole:Boolean) extends Neo4jWrapper with EmbeddedGraphDatabaseServiceProvider{
    
  def apply(dbLoc:String, outputFile:String, printToConsole:Boolean){
    registerShutdownHook
  }
  
  def neo4jStoreDir = dbLoc
  
  def registerShutdownHook = Runtime.getRuntime.addShutdownHook(new Thread() { override def run = shutdown })

  private def shutdown = {
    println("in shutdown - doing the trash")
    takeOutTrash
    println("executing shutdown")
    ds.gds.shutdown
  }
  
  /**
   * get the node right off of the reference node that we insert our query results into.
   * if we don't check the box in the GUI to sort in memory, we actually insert our results into 
   * a redblack tree and iteratively return them.  This (theoretically) allows more results than 
   * could fit into memory
   * 
   *  Any previously existing query results aren't immediately deleted.  They're given a 'trash' 
   *  relationship to the reference node, and collected later
   */
  def getQueryResultRootNode(createIfNecessary:Boolean = true):Neo4jRedBlackNode[String, Long] = {
    withTx[Neo4jRedBlackNode[String, Long]]{
      ds =>
        val existingRoot = Neo4jRedBlackNode.getRoot[String, Long](ds.gds, RelTypes.QUERY_RESULT)(s => DeDupingQueryResultWrapper(s))
        if(existingRoot != null){
          println("Found one or more previous queries - likely from an unclean shutdown.  Please give me time to clean it/them up.")
          while(Neo4jRedBlackNode.getRoot[String, Long](ds.gds, RelTypes.QUERY_RESULT)(s => DeDupingQueryResultWrapper(s)) != null){
            val qrNode = Neo4jRedBlackNode.getRoot[String, Long](ds.gds, RelTypes.QUERY_RESULT)(s => DeDupingQueryResultWrapper(s))
            qrNode.node.getSingleRelationship(RelTypes.QUERY_RESULT, Direction.INCOMING).delete
            ds.gds.getReferenceNode().createRelationshipTo(qrNode.node, RelTypes.TRASH)
          }
          //existingRoot.node.getSingleRelationship(RelTypes.QUERY_RESULT, Direction.INCOMING).delete
          //revisit this.  this could give us multiple 'query_result' trees
          Neo4jRedBlackNode.createRoot(ds.gds, RelTypes.QUERY_RESULT)
          Neo4jRedBlackNode.getRoot[String, Long](ds.gds, RelTypes.QUERY_RESULT)(s => DeDupingQueryResultWrapper(s))
        }
        else if(createIfNecessary){
          Neo4jRedBlackNode.createRoot(ds.gds, RelTypes.QUERY_RESULT)
          Neo4jRedBlackNode.getRoot[String, Long](ds.gds, RelTypes.QUERY_RESULT)(s => DeDupingQueryResultWrapper(s))          
        }
        else null
    }
  }
  

 /**
  * runs a shut down.  Deletes any query results that have been 'trashed' 
  */
  def takeOutTrash(){
    withTx{
      ds =>
        ds.gds.getReferenceNode().getRelationships(RelTypes.TRASH, Direction.OUTGOING).asScala.foreach{
          rel =>
            val tNode = Neo4jRedBlackNode[String, Long](rel.getEndNode())
            tNode.traverseForDelete.foreach{
              dNode:Node =>
                dNode.getRelationships().asScala.foreach(_.delete)
                dNode.delete
            }
        }
    }
  }

  
  /**
   * get the root node of the red black tree that contains entries for the start and end point of every datapoint in the file
   */
  def getBothTreeRootNode():Neo4jRedBlackNode[String, Long] = {
    withTx[Neo4jRedBlackNode[String, Long]]{
      ds =>
        Neo4jRedBlackNode.getRoot[String, Long](ds.gds, RelTypes.ALL_NODES_BOTH)(s => DeDupingQueryResultWrapper(s))
    }
  }

  
  
  
  /**
   * actually run the query
   * -- determine the start and end points from which to traverse the tree
   * -- either load those nodes into memory or a query result redblack tree depending on the parms passed in from the GUI
   * -- use the out-lier algo to find ranges that completely enclose the query range
   * -- sort results if they're in memory (they're sorted upon insertion if on disk)
   * -- return a lazy iterator of sorted results
   */
  def queryDb(queryResultRootNode:Neo4jRedBlackNode[String, Long], strtNode:GenomeDataNode, endNode:GenomeDataNode, fastAndEasySearch:Boolean, commitToDisk:Boolean = true, trxCount:Int = 1000):Iterator[GenomeDataNode] = {
    println("starting query")
    //registerShutdownHook
    println("registered shutdown hook")

    var tx = this.ds.gds.beginTx()
    
    val existingRoot = {
      if(queryResultRootNode == null && commitToDisk){
        getQueryResultRootNode(true)
      }
      else{
        queryResultRootNode
      }
    }
    
    val subsetToTraverse = Neo4jRedBlackNode.getRoot[String, Long](ds.gds, RelTypes.ALL_NODES_BOTH)(s => OrderedBigIntWrapper(s)).traverseSubset(strtNode.derivedStart.toString, endNode.derivedEnd.toString)
    
    if(commitToDisk){
      println("have query results, committing them to disk")
    }
    else{
      println("have query results, loading them into memory for sorting")
    }
    
    
    val numberedSubsetToTraverse = (1 to Int.MaxValue).iterator zip subsetToTraverse.iterator
    
    
    val resultSet:List[GenomeDataNode] = {
      if(commitToDisk) null
      else List[GenomeDataNode]()
    }
    
    
    //Neo4jRedBlackNode is we're sorting on disk, Set[GenomeDataNode] if in memory
    val lastQueryResultInsertThing = 
      if(commitToDisk){
        numberedSubsetToTraverse.foldLeft(existingRoot) {
        (redBlackNodeToInsert, dataTuplet:(Int, Node)) =>
          val dataNode:GenomeDataNode = getNodeById(dataTuplet._2.getProperty(PropKeys.VALUE).toString.toLong)(ds).toCC[GenomeDataNode].get
          val cont = redBlackNodeToInsert.insert(ds.gds, dataNode.value+"_"+dataTuplet._2.getProperty(PropKeys.VALUE), dataTuplet._2.getProperty(PropKeys.VALUE).toString.toLong, true)(s => DeDupingQueryResultWrapper(s))
          if(dataTuplet._1 % trxCount == 0){
            println("committing results at: "+trxCount)
            tx.success
            tx.finish
            tx = this.ds.gds.beginTx
          }
          cont
        }
      }
      else{
        numberedSubsetToTraverse.foldLeft(resultSet) {
        (setToWhichWeAppend, dataTuplet:(Int, Node)) =>
          val dataNode:GenomeDataNode = getNodeById(dataTuplet._2.getProperty(PropKeys.VALUE).toString.toLong)(ds).toCC[GenomeDataNode].get
          dataNode :: setToWhichWeAppend
        }
      }
    
    tx.success
    tx.finish
    tx = ds.gds.beginTx()
    
    if(commitToDisk){
      println("results are on disk, looking for out-liers if so requested.")
    }
    else{
      println("results are loaded into memory, looking for out-liers if so requested.")
    }
    
    
    val retIterator:Iterator[GenomeDataNode] = {
      val startTreeRoot:Neo4jRedBlackNode[String, Long] = Neo4jRedBlackNode.getRoot[String, Long](ds.gds, RelTypes.ALL_NODES_BOTH)(s => OrderedBigIntWrapper(s))
      if(!fastAndEasySearch){
        val mdNode:Neo4jRedBlackNode[String, String] = Neo4jRedBlackNode.getRoot[String, String](ds.gds, RelTypes.DB_MD)
        val maxLen:BigInt = BigInt(mdNode.node.getProperty(IntervalTreePropKeys.MAX_LENGTH).toString)
        
        val startPointToCheck = (BigInt(strtNode.derivedStart) - (maxLen + 1)).toString
        val startPointOfQuery = strtNode.derivedStart
        
        
        val queryResultRoot:Neo4jRedBlackNode[String, Long] = Neo4jRedBlackNode.getRoot[String, Long](ds.gds, RelTypes.QUERY_RESULT)(s => OrderedBigIntWrapper(s))
        
        
        lastQueryResultInsertThing match {
          case t:TREE[String, Long] => {
            val nodeWithRetData = startTreeRoot.traverseSubset(startPointToCheck.toString, startPointOfQuery).foldLeft(t){
              (queryResultNode, neoNodeWithData) =>
                val dataNode:GenomeDataNode = getNodeById(neoNodeWithData.getProperty(PropKeys.VALUE).toString.toLong)(ds).toCC[GenomeDataNode].get
                if(dataNode.derivedEnd >= startPointOfQuery){
                  TREE[String, Long](queryResultNode.insert(ds.gds, dataNode.value+"_"+neoNodeWithData.getProperty(PropKeys.VALUE), neoNodeWithData.getProperty(PropKeys.VALUE).toString.toLong, true)(s => DeDupingQueryResultWrapper(s)).node)
                }
                else{
                  queryResultNode
                }
            } 
            val ret = (for{n:Node <- nodeWithRetData.traverse}
              yield getNodeById(n.getProperty(PropKeys.VALUE).toString.toLong)(ds).toCC[GenomeDataNode].get).iterator
            println("done, cleaning up and committing transaction")
            nodeWithRetData.node.getSingleRelationship(RelTypes.QUERY_RESULT, Direction.INCOMING).delete
            ds.gds.getReferenceNode().createRelationshipTo(nodeWithRetData.node, RelTypes.TRASH)
            ret
          }
          case l:List[GenomeDataNode] => {
            startTreeRoot.traverseSubset(startPointToCheck.toString, startPointOfQuery).foldLeft(l){
              (queryResultNode, neoNodeWithData) =>
                val dataNode:GenomeDataNode = getNodeById(neoNodeWithData.getProperty(PropKeys.VALUE).toString.toLong)(ds).toCC[GenomeDataNode].get
                  if(dataNode.derivedEnd >= startPointOfQuery){
                    dataNode :: l
                  }
                  else{
                    l
                  }
            }.toSet[GenomeDataNode].toSeq.sortWith{
              (a, b) => a.compare(b) <= 0
            }.iterator
            
          }
        }
      }//if !fastNEasy
      else{
        lastQueryResultInsertThing match {
          case t:TREE[String, Long] => {
            val ret = (for{n:Node <- t.traverse}
              yield getNodeById(n.getProperty(PropKeys.VALUE).toString.toLong)(ds).toCC[GenomeDataNode].get).iterator
            println("done, cleaning up and committing transaction")
            t.node.getSingleRelationship(RelTypes.QUERY_RESULT, Direction.INCOMING).delete
            ds.gds.getReferenceNode().createRelationshipTo(t.node, RelTypes.TRASH)
            ret
          }
          case l:List[GenomeDataNode] => {
            l.toSet[GenomeDataNode].toSeq.sortWith{
              (a, b) => a.compare(b) <= 0
            }.iterator
          }
        }
      }
    }
      
    println("returning iterator of results")
        
    tx.success
    tx.finish
    
    return retIterator
     
  }  
}


