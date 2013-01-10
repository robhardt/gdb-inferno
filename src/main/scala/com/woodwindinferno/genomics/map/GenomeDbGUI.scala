package com.woodwindinferno.genomics.map

import swing._
import java.awt.Rectangle
import java.awt.Dimension
import scala.swing.ScrollPane
import GridBagPanel._
import java.awt.GridBagConstraints
import org.neo4j.graphdb.Node
import scala.io.Source
import java.io.BufferedWriter
import java.io.FileWriter
import java.io.File
import java.io.Writer
import scala.swing.event.Event
import scala.actors.Actor



/**
 * Genome Database GUI
 * This is just a graphical test harness for the Genome Database Querier.  It's not an end-user quality GUI.
 * It provides file choosers to point to a database and a directory for an output file (if requested)
 * It should probably be run with the -Xmx2048 flag, or with as much RAM as is readily available on the
 * user's workstation.
 */
object GenomeDbGUI extends SimpleSwingApplication {
  
  val MAX_ROWS_SHOWN_IN_GUI = 100

  var dbChooser = new FileChooser(new java.io.File("./"))
  dbChooser.fileSelectionMode = FileChooser.SelectionMode.FilesAndDirectories
  
  var dbLocation = "<Select the DB root directory>"
  var querier:GenomeDatabaseQuerier = null;
  var queryRootNode:Neo4jRedBlackNode[String, Long] = null

  // Channel Panel Construction: Channel Select
  val dbChooserInfoLabel = new Label {
    text = "Select the location of your DB:"
    horizontalAlignment = Alignment.Left
  }
  
  val dbNameLabel = new Label {
    text = dbLocation
    horizontalAlignment = Alignment.Left
  }

  val chooseDBButton = new Button {
    text = "Choose Database Location"
    horizontalAlignment = Alignment.Left
  }

  val queryLabel = new Label{
    text = "Query, ex (chr22:51243889-chrX:170421 or chr3:171402125-174758998)"
    horizontalAlignment = Alignment.Left
  }
  
  val queryTextArea = new TextField{
    preferredSize = new Dimension(180, 60)
  }
  
  val queryButton = new Button{
    text = "Execute Query"
    horizontalAlignment = Alignment.Left
  }
  
  val queryResultLabel = new Label{
    text = "execution details: "
    horizontalAlignment = Alignment.Left
  }
  
  val fastAndEasyCheckBox = new CheckBox{
    text = "run 'Fast & Easy' search"
      horizontalAlignment = Alignment.Right
  }
  
  val sortInMemoryCheckBox = new CheckBox{
    text = "sort in memory (faster for smaller result sets)"
      horizontalAlignment = Alignment.Right
      selected = true
  }
  
  
  val fileToSaveToLabel = new Label{
    text = "<enter file to save to>"
  }
  
  val fileToSaveButton = new Button{
    text = "Select Output File"
  }
  
  var saveToFileChooser = new FileChooser(new java.io.File("./"))
  saveToFileChooser.fileSelectionMode = FileChooser.SelectionMode.FilesAndDirectories
  var fileToSaveTo = "";
  
  


  val dbPanel = new BoxPanel(Orientation.Vertical) {
    contents += dbChooserInfoLabel
    contents += dbNameLabel
    contents += chooseDBButton
    contents += queryLabel
    contents += queryTextArea
    contents += queryButton
    contents += fastAndEasyCheckBox
    contents += sortInMemoryCheckBox
    contents += fileToSaveToLabel
    contents += fileToSaveButton
    border = Swing.EmptyBorder(10, 10, 10, 10)
  }
  
  val resultsLabel = new Label{
    text = "Query Results:"
    horizontalAlignment = Alignment.Left
    preferredSize = new Dimension(100, 50)
  }
  
  val resultsTextArea = new TextArea(){
    preferredSize = new Dimension(180, 720)
  }
  
  
  val insets:Insets = new Insets(0,0,0,0)
  
  val resultsSP = new ScrollPane(resultsTextArea){
    preferredSize = new Dimension(180,720)
  }
  val resultsPanel = new GridBagPanel(){    
    add(resultsLabel, new Constraints(0,0,1,1,0,0,GridBagConstraints.NORTHWEST, GridBagConstraints.NONE, insets, 10, 10){
      fill = Fill.None
    })
    add(resultsSP, new Constraints(0,1,10,10,1,1,GridBagConstraints.SOUTH, GridBagConstraints.BOTH, insets, 10, 10))
  }
  
  
  
  
  val outerSplitPane = new SplitPane(Orientation.Horizontal, dbPanel, resultsPanel)

  def top = new MainFrame {
    
    title = "Genome DB Front End"
    preferredSize = new Dimension(720, 1000)
    listenTo(chooseDBButton, queryButton, fileToSaveButton)
    reactions += {
      case swing.event.ButtonClicked(xButton) => {
        if(xButton == chooseDBButton){
          if(dbChooser.showOpenDialog(dbPanel) ==
              FileChooser.Result.Approve) {
                dbLocation = dbChooser.selectedFile.getAbsolutePath
                dbNameLabel.text = dbLocation
                querier = new GenomeDatabaseQuerier(dbLocation, null, true)
          }          
        }
        else if(xButton == queryButton){
          if(querier != null){
            runQuery            
          }
        }
        else if(xButton == fileToSaveButton){
          if(saveToFileChooser.showOpenDialog(dbPanel) ==
              FileChooser.Result.Approve) {
                fileToSaveTo = saveToFileChooser.selectedFile.getAbsolutePath.toString()
                if(new java.io.File(fileToSaveTo).isDirectory()){
                  fileToSaveTo = fileToSaveTo+java.io.File.separator+"results.txt"
                  fileToSaveToLabel.text = fileToSaveTo
                }
          }
        }
      }
    }
    
    // Overall construction
    contents = new BoxPanel(Orientation.Horizontal) {
      contents += outerSplitPane
      border = Swing.EmptyBorder(10, 10, 10, 10)
    }
    
    def runQuery ={
      resultsTextArea.text = ""
      val queryNodes = GenomeDatabaseBuilder.parseQuery(queryTextArea.text.trim)
      val resultsIter = (1 to Int.MaxValue).iterator zip querier.queryDb(null, queryNodes._1, queryNodes._2, fastAndEasyCheckBox.selected, !sortInMemoryCheckBox.selected, 1000)
      var fileWriter:Writer = null;
      val actualFile = {
        if(fileToSaveTo != null){
          fileWriter = new BufferedWriter(new FileWriter(new File(fileToSaveTo)))
        }
        else null
      }
      for(r <- resultsIter) {
        if(r._1 < MAX_ROWS_SHOWN_IN_GUI) resultsTextArea.append(r._2 + "\n")
        else if(r._1 == MAX_ROWS_SHOWN_IN_GUI) resultsTextArea.append("... and more")
        if(fileToSaveTo != null){
          fileWriter.write(r._2 + "\n")
        }
      }
      if(fileWriter != null) fileWriter.close()
    }
    
    override def closeOperation() = {
      if(querier != null){
        try{
          querier.shutdown(querier.ds)
        }
        catch{
          case _ => null
        }
      }
      super.closeOperation
    }
 
    
  }
}


case class Send(event: Any)(implicit intermediator: Intermediator) {
  intermediator ! this
}
case class Receive(event: Any) extends Event

case class Intermediator(application: Actor) extends Actor with Publisher {
  start()

  def act() {
    loop {
      react {
        case Send(evt) => application ! evt
        //case evt => onEDT(publish(Receive(evt)))
      }
    }
  }
}
