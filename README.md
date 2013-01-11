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
*   Move to a distributed client/server architecture
*   memory optimizations for creating a database
*   error handling
*   logging


To get started, download the binary runnable jar and a sample database, then execute:
java -Xmx2048m -jar gdb-inferno-0.0.2-SNAPSHOT-jar-with-dependencies.jar

This will bring up the GUI which will allow you to:

*  point to your database
*  specify an output file if desired
*  set some parameters
*  enter a query in the form chr21:100000-chrX:100000

Only the first 100 results will show up in the Swing GUI due to memory limitations.  All results will go in to the 'results.txt' file.



================

##Known Issues

1.  There is a limitation to the file chooser in Swing.  To specify the database, which is a file system directory, navigate to that directory's PARENT, and then select the database root filder, and click 'Continue'
2.  To specify an output file, specify a directory in the same fashion as in number 1.  A file called 'results.txt' will be created in that directory.
3.  When 'sort in memory' is selected, every search is a 'Fast & Easy' search.  The out-lier algorithm is broken when you select 'sort in memory.'  To work around this, just unselect 'sort in memory'


##GUI Features

1.  Fast & Easy Search:  This runs just a single time through the redblack tree.  It will not pick up ranges that completely enclose the range specified in your query.  To see those results, uncheck this box
2.  Sort in memory:  Leave this checked unless you know you'll have more results than you can sort in memory.  If this is unchecked, results are inserted into a redblack tree, which is then traversed to get them in sorted order.  This can be very slow for big result sets, but should theoretically allow you to return very large result sets if desired.

## Current Files
* Runnable Jar: https://s3.amazonaws.com/robhome/home/ec2-user/genome/gdb-inferno-0.0.2-SNAPSHOT-jar-with-dependencies.jar
* Sample Database: https://s3.amazonaws.com/robhome/home/ec2-user/genome/db1.zip


##Acknowledgements and Miscellany
*  The starting point for my red-black tree implementation came from here:  http://matt.might.net/articles/implementation-of-immutable-purely-functional-okasaki-red-black-tree-maps-in-scala/
*  The underlying database engine is Neo4j, which is available under a dual-license arrangement: Neo Technology Commercial License (NTCL), or the (A)GPLv3
http://www.neo4j.org/

