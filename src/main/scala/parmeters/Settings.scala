package parmeters

import geotrellis.Weight
import org.apache.spark.SparkConf

/**
  * Created by marc on 12.05.17.
  */
class Settings extends Serializable with Cloneable{

  var test = false
  var zoomlevel = 1

  var aggregationLevel = 1

  var scenario : String = Scenario.NoScenario.toString
  var multiToInt = 1000000
  //40.701915, -74.018704
  //40.763458, -73.967244
  var buttom = (40.699607, -74.020265)
  var top = (40.769239, -73.948286)
  //val top = (40.769239+0.010368, -73.948286+0.008021)
  var shiftToPostive = -1*buttom._2*multiToInt
  var latMin = buttom._1*multiToInt//Math.max(file.map(row => row.lat).min,40.376048)
  var lonMin = buttom._2*multiToInt+shiftToPostive//Math.max(file.map(row => row.lon).min,-74.407877)
  var latMax = top._1*multiToInt//Math.min(file.map(row => row.lat).max,41.330106)
  var lonMax = top._2*multiToInt+shiftToPostive//Math.min(file.map(row => row.lon).max,-73.292793)
  var sizeOfRasterLat = 100 //meters
  var sizeOfRasterLon = 100 //meters
  var rasterLatLength = ((latMax-latMin)/sizeOfRasterLat).ceil.toInt
  var rasterLonLength = ((lonMax-lonMin)/sizeOfRasterLon).ceil.toInt
  var weightMatrix = Weight.Square
  var weightRadius = 10
  var weightRadiusTime = 1
  var fromFile = true
  var clusterRange = 1.0
  var critivalValue = 5
  var focal = false
  var focalRange = weightRadius+20
  var focalRangeTime = weightRadiusTime+1
  var parent = true

  var serilizeDirectory = "/home/marc/media/SS_17/output/raster"
  var statDirectory = "/home/marc/media/SS_17/output/"

  var ouptDirectory = "/home/marc/media/SS_17/output/"


  var inputDirectoryCSV = "/home/media/Downloads/"
  var csvMonth = 1
  var csvYear = 2016
  var hour = 0
  var spaceTime = true

  var layoutTileSize: (Int,Int) = (300,300)
  val conf = new SparkConf().setAppName("Test")
  conf.setMaster("local[*]")

  def runToStringHead() : String ={
    "Versuchsnummer,A,Wr,Wz,Fa,Fc,Z"
  }

  def runToString() : String = {
    aggregationLevel+","+weightRadius+","+weightRadiusTime+","+focalRange+","+focalRangeTime+","+zoomlevel
  }
  override def toString: String = {
    scenario+","+sizeOfRasterLat+","+weightRadius+","+weightRadiusTime+","+focalRange+","+focalRangeTime+","+csvMonth
  }

  def getHeader(): String ={
    "Secnario,pixelSize,weight,weightTime,focal,focalTime,csvMonth"
  }
}
