package com.woodwindinferno.genomics.map

import org.neo4j.graphdb.Node
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.kernel.EmbeddedGraphDatabase
import org.neo4j.graphdb.RelationshipType
import org.neo4j.graphdb.Transaction
import scala.collection.JavaConverters._
import org.neo4j.graphdb.Direction
import Colors._
import PropKeys._
import RelTypes._
import Neo4jRedBlackNode._
import org.neo4j.graphdb.Relationship

/**
 * Represents a node in a red-black tree that can persist itself to a neo4j graph database
 * 
 * it implements getting the key & value, inserting, and traversing.  No deletes.
 * It was necessary to walk a tightrope between this object, which is immutable, and
 * the underlying Neo4jNodes, which I treat as mutable.
 * 
 *  When I tried to take an immutable approach to the underlying nodes, the database performance was
 *  unacceptable.  It couldn't tolerate the constant insertion/deletions/
 * 
 * @author rob
 *
 * @param <K>
 * @param <V>
 */
abstract class Neo4jRedBlackNode[K,V](val node: Node)(implicit cmp : K => Ordered[K]) {
	
  implicit val gdb:GraphDatabaseService = node.getGraphDatabase()
  
  def key:K = {
      node.getProperty(KEY).asInstanceOf[K]
  }
  
  def value:V = {
      node.getProperty(VALUE).asInstanceOf[V]
  }
  
  /**
   * insert a new node into the subtree consisting of this node and all of it's children
   */
  def insert(db:GraphDatabaseService, k:K, v:V, discardDuplicates:Boolean = false)(implicit cmp : K => Ordered[K]):Neo4jRedBlackNode[K,V] = {
    //println("inserting key: "+k+" into me. My Id: "+node.getId());
    makeBlack(_insert(db, k,v, discardDuplicates)(cmp))
  }
  
  /**
   * traverse this node's subtree, depth-first, inorder, returning just the keys
   */
  def traverseKeys:Stream[K] = {
    val amLeaf:Boolean = isLeaf(gdb, this.node)
    val ident = {
      if(amLeaf) "[LEAF]"
      else this.key
    }
    //println("in node: "+ident)
    if(amLeaf) Stream[K]()
    else left.traverseKeys ++ Stream[K](this.key) ++ right.traverseKeys
  }
  
  /**
   * traverse this node's subtree, depth-first, inorder, returning the underlying Neo4j nodes
   */
  def traverse:Stream[Node] = {
    val amLeaf:Boolean = isLeaf(gdb, this.node)
    val ident = {
      if(amLeaf) "[LEAF]"
      else this.key
    }
    //println("in node: "+ident)
    if(amLeaf) Stream[Node]()
    else left.traverse ++ Stream[Node](this.node) ++ right.traverse
  }
  
  /**
   * traverse in a different order that facilitates deletion
   */
  def traverseForDelete:Stream[Node] = {
    val amLeaf:Boolean = isLeaf(gdb, this.node)
    val ident = {
      if(amLeaf) "[LEAF]"
      else this.key
    }
    //println("in node: "+ident)
    if(amLeaf) Stream[Node]()
    else left.traverse ++ right.traverse ++ Stream[Node](this.node) 
  }
  
  
  /**
   * Traverse the subtree of this node starting with the first node with a key >= startVal
   * and stopping with the last node with a key <= endVal
   */
  def traverseSubset(startVal:K, endVal:K):Stream[Node] = {
    val amLeaf:Boolean = isLeaf(gdb, this.node)
    //println("in node: "+ident)
    if(amLeaf) Stream[Node]()
    else if(this.key < startVal) right.traverseSubset(startVal, endVal)
    else if(this.key <= endVal) left.traverseSubset(startVal, endVal) #::: Stream[Node](this.node) #::: right.traverseSubset(startVal, endVal)
    else left.traverseSubset(startVal, endVal)
    
  }
  
  /**
   * returns the parent of this node in the Redblack tree
   */
  def parent:Neo4jRedBlackNode[K,V] = {
    val parntRel = this.node.getSingleRelationship(PARENT, Direction.OUTGOING)
    if(parntRel != null) TREE[K,V](parntRel.getEndNode())
    else null
  }
  
  def color:Colors
  def left:Neo4jRedBlackNode[K,V]
  def right:Neo4jRedBlackNode[K,V]
  def _insert(db:GraphDatabaseService, k:K, v:V, discardDuplicates:Boolean = false)(implicit cmp : K => Ordered[K]):Neo4jRedBlackNode[K,V]
  
}


/**
 * Node implementation representing an end-leaf (black, no children)
 */
case class LEAF[K,V] (override val node:Node) (implicit cmp: K => Ordered[K]) extends Neo4jRedBlackNode[K,V](node:Node){
  
  def color:Colors = Colors.BLACK
  
  def left:Neo4jRedBlackNode[K,V] = null;
  def right:Neo4jRedBlackNode[K,V] = null;
  
  def _insert(db:GraphDatabaseService, k:K, v:V, discardDuplicates:Boolean = false)(implicit cmp : K => Ordered[K]):Neo4jRedBlackNode[K,V] = {
    
        val leftLeaf:Node = db.createNode()
        val rightLeaf:Node = db.createNode()     
        //println("creating a new tree with left and right nodes: "+leftLeaf.getId()+" / "+rightLeaf.getId())
        Neo4jRedBlackNode(this.node, RED, leftLeaf, rightLeaf, k, v)
    
  }
  
}



/**
 * Node implementation representing a tree (red or black, exactly 2 children)
 */
case class TREE[K,V](override val node:Node) (implicit cmp: K => Ordered[K]) extends Neo4jRedBlackNode[K,V](node:Node){
  
  def color:Colors = {
        node.getProperty(COLOR).toString() match {
          case "RED" => RED
          case _ => BLACK
        }
  }
  
  def left:Neo4jRedBlackNode[K,V] = _getChild(RelTypes.LEFT)
  
  def right:Neo4jRedBlackNode[K,V] = _getChild(RelTypes.RIGHT)

  private def _getChild(rt:RelTypes.RelTypes):Neo4jRedBlackNode[K,V] = {
    //println("in TREE getChild, myId: "+this.node.getId())
    val n:Node = 
        node.getSingleRelationship(rt, Direction.OUTGOING).getEndNode()
    isLeaf(n.getGraphDatabase(), n) match {
      case true => LEAF(n)
      case false => TREE(n)
    }
  }
  
  def _insert(db:GraphDatabaseService, k:K, v:V, discardDuplicates:Boolean = false)(implicit cmp : K => Ordered[K]):Neo4jRedBlackNode[K,V] = {
    //println("in TREE insert.  My key: "+key+", my ID: "+node.getId()+", inserting key: "+k);
    val ret = {
      if(k < key){
        //println("key is <=, going to insert left and balance")
        balance(db, color, left._insert(db, k, v, discardDuplicates).node, key, value, right.node)
      }
      else if (k == key){
        if(discardDuplicates) this
        else balance(db, color, left._insert(db, k, v, discardDuplicates).node, key, value, right.node)
      }
      else{
        //println("key is >, going to insert right and balance")
        balance(db, color, left.node, key, value, right._insert(db, k, v, discardDuplicates).node)
      }
    }
    ret
  }
  
  def balance(db:GraphDatabaseService, c:Colors, leftNode:Node, keyArg:K, valArg:V, rightNode:Node):Neo4jRedBlackNode[K,V] = {
    if(c == BLACK){
      if(left.color == RED && left.left != null && left.left.color == RED){
              val a:Node = left.left.left.node
              val xK = left.left.key
              val xV = left.left.value
              val b:Node = left.left.right.node
              val yK = left.key
              val yV = left.value
              val c = left.right.node
              val zK = keyArg
              val zV = valArg
              val d = right.node
              _balance(db, a, xK, xV, b, yK, yV, c, zK, zV, d, this.node :: left.left.node :: left.node :: Nil)            
      }
      else if(left.color == RED && left.right != null && left.right.color == RED){
              val a:Node = left.left.node
              val xK = left.key
              val xV = left.value
              val b = left.right.left.node
              val yK = left.right.key
              val yV = left.right.value
              val c = left.right.right.node
              val zK = keyArg
              val zV = valArg
              val d = right.node
              _balance(db, a, xK, xV, b, yK, yV, c, zK, zV, d, this.node :: left.node :: left.right.node :: Nil)
        }
        else if(right.color == RED && right.left != null && right.left.color == RED){
              val a:Node = left.node
              val xK = keyArg
              val xV = valArg
              val b = right.left.left.node
              val yK = right.left.key
              val yV = right.left.value
              val c = right.left.right.node
              val zK = right.key
              val zV = right.value
              val d = right.right.node
              _balance(db, a, xK, xV, b, yK, yV, c, zK, zV, d, this.node :: right.left.node :: right.node :: Nil)
        }
        else if(right.color == RED && right.right != null && right.right.color == RED){
              val a:Node = left.node
              val xK = keyArg
              val xV = valArg
              val b = right.left.node
              val yK = right.key
              val yV = right.value
              val c = right.right.left.node
              val zK = right.right.key
              val zV = right.right.value
              val d = right.right.right.node
              _balance(db, a, xK, xV, b, yK, yV, c, zK, zV, d, this.node :: right.node :: right.right.node :: Nil)
        }
      else this
    }
    else this
  }
  

  
  //assumes we're in a transaction!
  private def _balance(db:GraphDatabaseService, a: Node, xK: K, xV: V, b: Node, yK:K, yV: V, c:Node, zK:K, zV:V, d:Node, nodesToDelete:List[Node]):Neo4jRedBlackNode[K,V] ={
    //println("**BALANCE MATCH**")
    //println(getKey(a)+" : "+xK+" : "+getKey(b)+" : "+yK+" : "+getKey(c)+" : "+zK+" : "+getKey(d))
    
    val newLeftNode = db.createNode()
    val newLeft = Neo4jRedBlackNode(newLeftNode, BLACK, a, b, xK, xV)
    
    val newRightNode = db.createNode()
    val newRight = Neo4jRedBlackNode(newRightNode, BLACK, c, d, zK, zV)
    
    val newParentNode = db.createNode()
    val newParent = Neo4jRedBlackNode(newParentNode, RED, newLeftNode, newRightNode, yK, yV)

    for(rel <- node.getRelationships(Direction.INCOMING).asScala){
      val start = rel.getStartNode()
      start.createRelationshipTo(newParent.node, rel.getType())
    }
    
    val parntRel:Relationship = node.getSingleRelationship(PARENT, Direction.OUTGOING)
    
    if(parntRel != null) newParent.node.createRelationshipTo(parntRel.getEndNode(), PARENT);
        
    nodesToDelete.map(_.getRelationships().asScala.map(_.delete))
    nodesToDelete.map(_.delete)
   
    newParent
    
  }

  
}



object Neo4jRedBlackNode{
  
  import Colors._
  import PropKeys._
  import RelTypes._
  import com.woodwindinferno.genomics.map.Colors
  import com.woodwindinferno.genomics.map.RelTypes
  import com.woodwindinferno.genomics.map.PropKeys
  
  def apply[K,V](node:Node)(implicit cmp : K => Ordered[K]):Neo4jRedBlackNode[K,V] = {
    if(isLeaf(node)){
      node.setProperty(COLOR, BLACK.toString)
      LEAF(node)
    }
    else{
      TREE(node)
    }
  }
  
  
  def apply[K,V](node:Node, c:Colors,  l:Node, r:Node, k:K, v:V)(implicit cmp : K => Ordered[K]):Neo4jRedBlackNode[K,V] = {
      //println("In TREE apply method, incoming node id: "+node.getId())
      
      node.getRelationships(Direction.OUTGOING, LEFT, RIGHT).asScala.map(_.delete)
      
      l.getRelationships(Direction.INCOMING, LEFT, RIGHT).asScala.map(_.delete)
      l.getRelationships(PARENT, Direction.OUTGOING).asScala.map(_.delete)
      r.getRelationships(Direction.INCOMING, LEFT, RIGHT).asScala.map(_.delete)
      r.getRelationships(PARENT, Direction.OUTGOING).asScala.map(_.delete)
      
      node.setProperty(KEY, k)
      node.setProperty(VALUE, v)
      node.setProperty(COLOR, c.toString())

      node.createRelationshipTo(l, LEFT)
      node.createRelationshipTo(r, RIGHT)
      
      //println("*** creating parent relationship from "+l+" to "+node)
      l.createRelationshipTo(node, PARENT)
      r.createRelationshipTo(node, PARENT) 
      
      val ret:Neo4jRedBlackNode[K,V] = TREE(node)
      ret
  }
  
  def createRoot[K,V](db:GraphDatabaseService, rootRelation:RelTypes):Neo4jRedBlackNode[K,V] = {
      val n:Node = db.createNode()
      db.getReferenceNode().createRelationshipTo(n, rootRelation)
      n.setProperty(COLOR, BLACK.toString())
      val l:LEAF[K,V] = LEAF[K,V](n)(k => k.asInstanceOf[Ordered[K]])
      l
  }
  
  def getRoot[K,V](db:GraphDatabaseService, rootRelation:RelTypes)(implicit cmp:K => Ordered[K]):Neo4jRedBlackNode[K,V] = {
    try{
      
      val tnRels:List[Relationship] = db.getReferenceNode().getRelationships(rootRelation, Direction.OUTGOING).asScala.toList
      
      val n:Node = tnRels.head.getEndNode()
      
      
      if(isLeaf(db, n)){
        val l:LEAF[K,V] = LEAF(n)(k => k.asInstanceOf[Ordered[K]])
        l
      }
      else{
        val t:TREE[K,V] = TREE(n)
        t
      }
    }
    catch{
      case _ => null
    }
  }
  
  def isLeaf(db:GraphDatabaseService, xNode:Node):Boolean ={
      val ret:Boolean = xNode.getRelationships(LEFT, Direction.OUTGOING).asScala.isEmpty
      ret
  }
  
  def isLeaf(xNode:Node):Boolean ={
      val ret:Boolean = xNode.getRelationships(LEFT, Direction.OUTGOING).asScala.isEmpty
      ret
  }
  
  
  private[map] def makeBlack[K,V](nodeArg:Neo4jRedBlackNode[K,V])(implicit cmp:K => Ordered[K]): Neo4jRedBlackNode[K,V] = {
    nodeArg match{
      case LEAF(node) => nodeArg
      case TREE(node) => {
        if(nodeArg.color == RED){
              node.setProperty(COLOR, BLACK.toString())
              nodeArg
        }
        else nodeArg
      }
    }
  }

}