package scripts

import exportstructure.SerializeTile
import geotrellis.raster.Tile
import importExport.ImportGeoTiff
import parmeters.{Scenario, Settings}
import rasterTransformation.Transformation

/**
  * Created by marc on 14.06.17.
  */
object Generate {

  def exportAsTiffRaster(): Unit = {

  }

  def printRaster(): Unit = {
    val globalSettings = new Settings
    val geoTiff = new ImportGeoTiff()
    globalSettings.scenario = Scenario.Script.toString
    globalSettings.sizeOfRasterLat = 100
    globalSettings.sizeOfRasterLon = 100
    globalSettings.fromFile =true
    var raster : Tile = getRaster(globalSettings)
    geoTiff.writeGeoTiff(raster,globalSettings, "Manhatten")
    val buttom = (40.5,-73.7)
    val top = (40.9,-47.25)
    val multiToInt = globalSettings.multiToInt
    val shiftToPostive = 47.25*globalSettings.multiToInt
    globalSettings.latMin = buttom._1*multiToInt//Math.max(file.map(row => row.lat).min,40.376048)
    globalSettings.lonMin = buttom._2*multiToInt+shiftToPostive//Math.max(file.map(row => row.lon).min,-74.407877)
    globalSettings.latMax = top._1*multiToInt//Math.min(file.map(row => row.lat).max,41.330106)
    globalSettings.lonMax = top._2*multiToInt+shiftToPostive//Math.min(file.map(row => row.lon).max,-73.292793)
    globalSettings.sizeOfRasterLat = 400
    globalSettings.sizeOfRasterLon = 400
    globalSettings.fromFile =true
    raster = getRaster(globalSettings)
    geoTiff.writeGeoTiff(raster,globalSettings, "All")

  }

  def main(args: Array[String]): Unit = {
    println("Started")
    printRaster()
  }

  def getRaster(settings : Settings): Tile = {
    val serializer = new SerializeTile(settings.serilizeDirectory)
    var raster : Tile = null
    if(settings.fromFile){
      raster = creatRaster(settings)
    } else {
      raster = serializer.read()
    }
    raster
  }

  def creatRaster(settings : Settings): Tile = {
    val transform = new Transformation
    val arrayTile = transform.transformCSVtoRaster(settings)
    arrayTile
  }
}
