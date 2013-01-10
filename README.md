gdb-inferno
===========

Genomic Data Range Database

gdb-inferno is a range-query database based on Neo4j.  It is designed to facilitate searching ranges over large sets of data.

It currently allows you to:

*  Parse an input file and insert it into a custom Neo4j Database
*  run command-line queries against this custom database
*  browse the data via a basic Swing GUI

============

It is currently still in its proof-of-concept state, and only supports a single file format.  The following are some possible future enhancements:

*   an end-user-quality GUI
*   memory optimizations for creating a database
*   error handling
*   logging


To use it, download the binary runnable jar and a sample database
java -Xmx2048m -jar WoodwindInfernoGenomicDB-0.0.1-SNAPSHOT-jar-with-dependencies.jar

This will bring up the GUI which will allow you to:

*  point to your database
*  specify an output file if desired
*  set some parameters
*  enter a query in the form chr21:100000-chrX:100000

Only the first 100 results will show up in the Swing GUI due to memory limitations.  All results will go in to the 'results.txt' file.



================

##Known Issues
