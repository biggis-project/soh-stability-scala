package exportstructure

import java.io.PrintWriter

import geotrellis.raster.Tile

import scala.collection.mutable.ListBuffer

/**
  * Created by marc on 11.05.17.
  */
class SoHResultTabell {

  def printResults(results : ListBuffer[SoHResult], shortFormat: Boolean): Unit ={
    val resultsSorted = results.sortWith((x,y)=> x.getLat()<y.getLat())
    var line = ""
    var headerLine =""
    var header = true
    var counter = resultsSorted.head.getLat()
    for(r <- resultsSorted){
      if(counter==r.getLat()){
        if(header){
          headerLine += r.header(shortFormat)
        }
        line += r.format(shortFormat)
      }else {
          if(header){
            println(headerLine)
          }
          header=false
          println(line)
          line = r.format(shortFormat)
          counter = r.getLat()
      }
    }
    println(line)
  }

  def printResults(results : ListBuffer[SoHResult], shortFormat: Boolean, writer : PrintWriter): Unit ={
    val resultsSorted = results.sortWith((x,y)=> x.getLat()<y.getLat())
    var line = ""
    var headerLine =""
    var header = true
    var counter = resultsSorted.head.getLat()
      for(r <- resultsSorted){
        if(counter==r.getLat()){
          line += r.format(shortFormat)
          if(header){
            headerLine += r.header(shortFormat)
          }
        } else {
          if(header){
            writer.println(headerLine)
          }
          header=false
          writer.println(line)
          line = r.format(shortFormat)
          counter = r.getLat()
        }
      }
    writer.println(line)
  }


  def printResultsList(results : ListBuffer[SoHResult]): Unit ={
    for(r <- results){
      println((r.localSet.focalRange)+","+(r.localSet.weightRadius)+","+(r.localSet.aggregationLevel)+","+r.getSohUp()+","+r.getSohDown())
    }
  }

  def getFormatedResultsListShort(results : ListBuffer[SoHResult]): String ={
    var out = ""
    for(r <- results){
      out += ((r.localSet.focalRange)+","+(r.localSet.weightRadius)+","+(r.localSet.aggregationLevel)+","+r.getSohUp()+","+r.getSohDown())+"\n"
    }
    out
  }


}
