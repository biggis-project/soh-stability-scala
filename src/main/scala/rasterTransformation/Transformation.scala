package rasterTransformation

import java.io.{File, PrintWriter}
import java.text.SimpleDateFormat
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import datastructure.{NotDataRowTransformation, NotDataRowTransformationValue, RowTransformationTime}
import geotrellis.raster.{Tile, _}
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.hadoop._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import geotrellis.vector._
import org.apache.hadoop.mapred.{FileInputFormat, JobConf}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import parmeters.Settings

import scala.collection.immutable.TreeMap
import scala.io.Source
import scalaz.stream.nio.file
/**
  * Created by marc on 21.04.17.
  */
class Transformation {

  def transformCSVtoRaster(settings : Settings): IntArrayTile ={
    //https://www.google.com/maps/place/40%C2%B033'06.6%22N+74%C2%B007'46.0%22W/@40.7201276,-74.0195387,11.25z/data=!4m5!3m4!1s0x0:0x0!8m2!3d40.551826!4d-74.129441
    //lat = 40.551826, lon=-74.129441
    //https://www.google.com/maps/place/40%C2%B059'32.5%22N+73%C2%B035'51.3%22W/@40.8055274,-73.8900207,10.46z/data=!4m5!3m4!1s0x0:0x0!8m2!3d40.992352!4d-73.597571
    //lat =40.992352, lon=-73.597571
    //lat = lat_min-lat_max = 440526 = 47, lon = lon_min-lon_max =531870 = 50 km => measurements approximately in meters

    //other values https://www.deine-berge.de/Rechner/Koordinaten/Dezimal/40.800296,-73.928375
    //40.800296, -73.928375
    //40.703286, -74.019012

    // val bufferedSource = Source.fromFile(settings.inputDirectoryCSV+settings.csvYear+"_"+settings.csvMonth+".csv")
    transformCSVtoRasterParametrised(settings,settings.inputDirectoryCSV+settings.csvYear+"_"+settings.csvMonth+".csv",5,6,1)
  }

  def transformCSVtoTimeRaster(settings : Settings): ArrayMultibandTile ={
    //transformCSVtoTimeRasterParametrised(settings,settings.inputDirectoryCSV+"in.csv",5,6,2)
    transformCSVtoTimeRasterParametrised(settings,settings.inputDirectoryCSV+settings.csvYear+"_"+settings.csvMonth+".csv",5,6,1)
  }

  def transformCSVtoRasterParametrised(settings : Settings, fileName : String, indexLon : Int, indexLat : Int, indexDate : Int): IntArrayTile ={
    val bufferedSource = Source.fromFile(fileName)
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    val file = bufferedSource.getLines.drop(1).map(line => {
      val cols = line.split(",").map(_.trim)
      val result = new NotDataRowTransformation(0,0,null,true)
      if(cols.length>Math.max(indexDate,Math.max(indexLat,indexLon))){
        result.lon = (cols(indexLon).toDouble*settings.multiToInt+settings.shiftToPostive).toInt
        result.lat = (cols(indexLat).toDouble*settings.multiToInt).toInt
        result.time =LocalDateTime.from(formatter.parse(cols(indexDate)))
        result.data = false
      }
      result
    }).filter(row => row.lon>=settings.lonMin && row.lon<=settings.lonMax && row.lat>=settings.latMin && row.lat<=settings.latMax && row.data==false)

    val rasterLatLength = ((settings.latMax-settings.latMin)/settings.sizeOfRasterLat).toInt
    val rasterLonLength = ((settings.lonMax-settings.lonMin)/settings.sizeOfRasterLon).toInt
    val tile = IntArrayTile.ofDim(rasterLonLength,rasterLatLength)
    var colIndex = 0
    var rowIndex = 0
    for(row <- file){
      colIndex = ((row.lon-settings.lonMin)/settings.sizeOfRasterLon).toInt
      rowIndex = tile.rows-((row.lat-settings.latMin)/settings.sizeOfRasterLat).toInt-1
      if(rowIndex >= 0 && colIndex >=0 && colIndex < tile.cols && rowIndex < tile.rows){
        tile.set(colIndex,rowIndex,tile.get(colIndex,rowIndex)+1)
      } else {
        //println("Not in range")
      }

    }
    bufferedSource.close()
    tile
  }

  def transformCSVtoTimeRasterParametrised(settings : Settings, fileName : String, indexLon : Int, indexLat : Int, indexDate : Int): ArrayMultibandTile ={
    val bufferedSource = Source.fromFile(fileName)
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    val file = bufferedSource.getLines.drop(1).map(line => {
      val cols = line.split(",").map(_.trim)
      val result = new NotDataRowTransformation(0,0,null,true)
      if(cols.length>Math.max(indexDate,Math.max(indexLat,indexLon))){
        result.lon = (cols(indexLon).toDouble*settings.multiToInt+settings.shiftToPostive).toInt
        result.lat = (cols(indexLat).toDouble*settings.multiToInt).toInt
        result.time =LocalDateTime.from(formatter.parse(cols(indexDate)))
        result.data = false
      }
      result
    }).filter(row => row.lon>=settings.lonMin && row.lon<=settings.lonMax && row.lat>=settings.latMin && row.lat<=settings.latMax && row.data==false)
    val multibandTile = new Array[IntArrayTile](24)


    var rasterLatLength = ((settings.latMax-settings.latMin)/settings.sizeOfRasterLat.toDouble).ceil.toInt
    var rasterLonLength = ((settings.lonMax-settings.lonMin)/settings.sizeOfRasterLon.toDouble).ceil.toInt

    rasterLatLength = rounding(rasterLatLength)
    rasterLonLength = rounding(rasterLonLength)
    println("Raster(Lat,Long)"+rasterLatLength+","+rasterLonLength)
    assert(rasterLatLength%20==0 && rasterLonLength%20==0)
    var colIndex = 0
    var rowIndex = 0
    for (row <- file) {
        colIndex = ((row.lon - settings.lonMin) / settings.sizeOfRasterLon).toInt
        rowIndex = rasterLatLength - ((row.lat - settings.latMin) / settings.sizeOfRasterLat).toInt - 1
        if (rowIndex >= 0 && colIndex >= 0 && colIndex < rasterLonLength && rowIndex < rasterLatLength) {
          val index = row.time.getHour
          if(multibandTile(index)==null){
            multibandTile(index)=IntArrayTile.ofDim(rasterLonLength,rasterLatLength)
          }
          (multibandTile(index)).set(colIndex, rowIndex, (multibandTile(index)).get(colIndex, rowIndex) + 1)
        } else {
          //println("Not in range")
        }

    }
    bufferedSource.close()
    new ArrayMultibandTile(multibandTile.map(arrayTile => arrayTile.toArrayTile()))
  }

  private def rounding(rasterLatOrLongLength: Int): Int = {
    if (rasterLatOrLongLength % 20 != 0) {
      if (rasterLatOrLongLength % 20 < 10) {
        return rasterLatOrLongLength - (rasterLatOrLongLength % 20)
      } else {
        return rasterLatOrLongLength - ((rasterLatOrLongLength+20) % 20)
      }
    }
    return rasterLatOrLongLength
  }




}
