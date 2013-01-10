package com.woodwindinferno.genomics.test


import org.scalatest.FunSuite
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/**
 * This class is a test suite for the methods in object FunSets. To run
 * the test suite, you can either:
 *  - run the "test" command in the SBT console
 *  - right-click the file in eclipse and chose "Run As" - "JUnit Test"
 */
@RunWith(classOf[JUnitRunner])
class GenomicDBTestSuite extends FunSuite {

    test("xx") {
    	assert(true)
    }
    
//    test("build me a tree") {
//    	val root:RBMap[Integer, String] = RBMap()
//    	val r1 = root.insert(0, "").insert(5, "").insert(10, "").insert(15, "").insert(20, "").insert(25, "").insert(30, "").insert(35, "").insert(40, "").insert(45, "");
//    	
//    	new Viz[Integer, String]().visualizeTree(r1);
//    	
//    	assert(root != null)
//    	
//    }

}