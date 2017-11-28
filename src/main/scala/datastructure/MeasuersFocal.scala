package datastructure

/**
  * Created by marc on 14.08.17.
  */
class MeasuersFocal(){
  var neighbour = 0.0
  var neighbourCount = 0
  var all = 0.0
  var noNeighbour = 0.0
  var noNeighbourCount = 0
  var row = 0.0
  var rowCount = 0
  var band = 0.0
  var bandCount = 0
  var writing = 0.0
  var broadCast = 0.0
  var collect = 0.0
  var stitch = 0.0

  def setcollect(time : Long): Unit ={
    collect = time
  }

  def setStitch(time : Long): Unit ={
    stitch = time
  }
  def addRow(time : Double): Unit ={
    row += time
    rowCount += 1
  }

  def addNeighbour(time : Double): Unit ={
    neighbour += time
    neighbourCount += 1
  }

  def addNoNeighbour(time : Double): Unit ={
    noNeighbour += time
    noNeighbourCount += 1
  }

  def addBand(time : Double): Unit ={
    band += time
    bandCount += 1
  }

  def setAll(all : Double): Unit = {
    this.all = all
  }

  def setWriting(time : Double): Unit = {
    this.writing = time
  }

  def setBroadCast(time : Long): Unit ={
    broadCast = time
  }

  def getPerformanceMetrik(): String ={
    val out = "Time for focal G* was:"+all+
      "\n time for neigbour:"+neighbour/neighbourCount.toDouble+
      "\n time for NoNeigbour:"+noNeighbour/noNeighbourCount.toDouble+
      "\n time for Band:"+band/bandCount.toDouble+
      "\n time for Row:"+row/rowCount.toDouble+
      "\n time for Writing:"+writing+
      "\n time for Broadcast:"+broadCast+
      "\n time for collect:"+collect+
      "\n time for stitch:"+stitch
    out
  }
}
