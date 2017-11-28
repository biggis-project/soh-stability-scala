package importExport

import java.io.File
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import parmeters.Settings

import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.io.Source

/**
  * Created by marc on 05.06.17.
  * Used to specify where the data should be stored.
  */
object PathFormatter {

  def getResultDirectoryAndName(settings: Settings, resultType: ResultType.Value) : String = {
    if(settings.test){
      settings.ouptDirectory = "/tmp/"
    }
    var sub = "server1/"+settings.csvYear+"/"+settings.csvMonth+"/"+resultType+"/"
    if(settings.focal){
      sub += "focal/"
    } else {
      sub += "global/"
    }
    val dir = settings.ouptDirectory+"/"+sub
    val f = new File(dir)
    f.mkdirs()
    dir+"a"+settings.aggregationLevel+"_w"+settings.weightRadius+"_wT"+settings.weightRadiusTime+"_f"+settings.focalRange+"_fT"+settings.focalRangeTime+"_z"+settings.zoomlevel+"result.txt"
  }

  def getAllResultsFor(settings: Settings): ResultStore ={
    val results = new ResultStore(
    getResultDirectoryAndName(settings,ResultType.Metrik),
    getResultDirectoryAndName(settings,ResultType.Validation),
    getResultDirectoryAndName(settings,ResultType.Time))
    results
  }
  class ResultStore(metrik : String, validation : String, time : String){


    val bufferedSourceMetrik = Source.fromFile(metrik)
    val bufferedSourceValidation = Source.fromFile(validation)
    val bufferedSourceTime = Source.fromFile(time)

    def getMinMax() : (Double,Double) ={
      val validation = bufferedSourceValidation.reset().getLines().map(x=>x.toDouble).toSeq

      (validation.min,validation.max)
    }

    def getMedian(): Double ={
      val validation = bufferedSourceValidation.getLines().map(x=>x.toDouble).toSeq.sortWith(_>_)
      val halfSize = validation.size/2
      validation.drop(halfSize-1).head
    }

    def getMean() : Double ={
      var validation = bufferedSourceValidation.getLines().map(x => x.toDouble).toSeq
      var size = validation.length
      validation.toSeq.reduce(_+_)/size.toDouble
    }

    def getYear() : Double ={
      var validation = bufferedSourceValidation.getLines().map(x => x.toDouble).toSeq
      var size = validation.length
      validation.toSeq.take(4).head
    }

    def getMetrik(focal : Boolean): Array[(String,Array[Double])] ={
      val validations = bufferedSourceMetrik.getLines().drop(1).take(13).toSeq.map(x=>{
        val tuple = x.replace("(","").replace(")","").split(",")
        var res : (String,Array[Double]) = null
        if(tuple.size>2){
          if(!focal){
            res = (tuple(0), Array(tuple(1).toDouble, tuple(2).toDouble,
              1, 1,
              tuple(5).toDouble, tuple(6).toDouble))
          }else {
            res = (tuple(0), Array(tuple(1).toDouble, tuple(2).toDouble,
              tuple(3).toDouble, tuple(4).toDouble,
              tuple(5).toDouble, tuple(6).toDouble))
          }
        } else {
          res = (tuple(0),Array(tuple(1).toDouble))
        }
        res
      })
      val array = validations.toArray
      val result = new Array[(String,Array[Double])](array.length-3)
      var counter = 0
      for(i<- 0 to array.length-1){
        if(i==4 || i==9 || i==8){

        } else {
          result(counter) = array(i)
          counter+=1
        }
      }
      result
    }
    def getTime(): Array[(String,Double)] ={
      val time = bufferedSourceTime.getLines().map(x=>{
        var tuple = x.split(":")
        (tuple(0),tuple(1).toDouble)
      }).toSeq
      time.toArray
    }


  }


  def getDirectory(settings : Settings, extra : String): String ={
    if(settings.test){
      settings.ouptDirectory = "/tmp/"
    }
    val formatter = DateTimeFormatter.ofPattern("dd_MM")
    var sub = settings.csvYear+"/"+settings.csvMonth+"/"
    //var sub = "Time_"+LocalDateTime.now().format(formatter)+"/"
    if(extra.equals("raster")){
      sub += "Raster/"
    } else if(settings.focal){
      sub += "focal/"+extra+"/FocalRange_"+settings.focalRange+"/"
    } else {
      sub += "global/"+extra+"/"
    }
    val dir = settings.ouptDirectory+settings.scenario+"/"+sub
    val f = new File(dir)
    
    f.mkdirs()
    dir
  }

  def getDirectoryAndName(settings : Settings, tifType: TifType.Value): String ={
    getDirectoryAndName(settings,tifType,true)
  }

  def getDirectoryAndName(settings : Settings, tifType: TifType.Value, isMultiband : Boolean): String ={
    if(settings.test){
      settings.ouptDirectory = "/tmp/"
    }
    var sub = "GIS_Daten/Mulitband"+isMultiband+"/"+settings.csvYear+"/"+settings.csvMonth+"/"
    if(tifType==TifType.Raw){
      sub += "Raster/"
    } else if(settings.focal){
      sub += tifType+"/focal/"
    } else {
      sub += tifType+"/global/"
    }
    val dir = settings.ouptDirectory+"/"+sub
    val f = new File(dir)
    f.mkdirs()
    if(tifType==TifType.Raw){
      return dir+"a"+settings.aggregationLevel+"_z"+settings.zoomlevel+"_.tif"
    } else {
      return dir+"a"+settings.aggregationLevel+"_w"+settings.weightRadius+"_wT"+settings.weightRadiusTime+"_f"+settings.focalRange+"_fT"+settings.focalRangeTime+"_z"+settings.zoomlevel+".tif"
    }

  }

  def exist(settings: Settings, tifType: TifType.Value): Boolean ={
    exist(settings,tifType,true)
  }

  def exist(settings: Settings, tifType: TifType.Value, isMultiband : Boolean): Boolean ={
    (new File(getDirectoryAndName(settings,tifType, isMultiband))).exists()
  }


}

object TifType extends Enumeration {
  val Raw,GStar,Cluster = Value
}

object ResultType extends Enumeration {
  val Validation,Metrik,Time,HotSpots = Value
}
