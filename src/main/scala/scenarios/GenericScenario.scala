package scenarios

import java.io.{File, PrintWriter}
import java.time.LocalDateTime

import com.typesafe.scalalogging.LazyLogging
import exportstructure.{SerializeTile, SoHResult, SoHResultTabell, TileVisualizer}
import geotrellis.Weight
import geotrellis.raster.{MultibandTile, Tile}
import getisOrd.SoH.SoHR
import getisOrd.{GetisOrd, GetisOrdFocal}
import importExport.ImportGeoTiff
import parmeters.Settings
import rasterTransformation.Transformation

import scala.collection.mutable.ListBuffer


/**
  * Created by marc on 24.05.17.
  */
abstract class GenericScenario extends LazyLogging {

  def runScenario(): Unit ={
    val globalSettings =new Settings()
    globalSettings.fromFile = true
    globalSettings.weightMatrix = Weight.Square
    val outPutResults = ListBuffer[SoHResult]()
    val runs = 10

    forFocalG(globalSettings, outPutResults, runs)
    //forGlobalG(globalSettings, outPutResults, runs)
    saveResult(globalSettings, outPutResults)
  }


  def saveResult(settings: Settings, outPutResults: ListBuffer[SoHResult]): Unit = {
    val outPutResultPrinter = new SoHResultTabell()
    val dir = settings.ouptDirectory+settings.scenario+"/"
    val f = new File(dir)
    f.mkdirs()
    val pw = new PrintWriter(new File(dir+LocalDateTime.now().formatted("dd_MM___HH_mm_")+"result.csv"))
    outPutResultPrinter.printResultsList(outPutResults)
    outPutResults.map(x => pw.println(x.format(false)))
    pw.flush()
    pw.close()
    val pwShort = new PrintWriter(new File(dir+LocalDateTime.now().formatted("dd_MM___HH_mm_")+"short_result.csv"))
    outPutResultPrinter.printResults(outPutResults,true, pwShort)
    pwShort.flush()
    pwShort.close()
  }

  def saveSoHResults(totalTime: Long, outPutResults: ListBuffer[SoHResult], globalSettings: Settings, chs: ((Tile, Int), (Tile, Int)), sohVal: SoHR, lat : (Int,Int)): Unit = {
    val outPutResultPrinter = new SoHResultTabell()
    outPutResults += new SoHResult(chs._1._1,
      chs._2._1,
      globalSettings,
      ((System.currentTimeMillis() - totalTime) / 1000),
      sohVal,
      lat._1)
    val dir = globalSettings.ouptDirectory+globalSettings.scenario+"/"
    val pwShort = new PrintWriter(new File(dir+"focal_"+globalSettings.focal+"d3.csv"))
    pwShort.println("F,W,Z,Up,Down")
    pwShort.println(outPutResultPrinter.getFormatedResultsListShort(outPutResults))
    pwShort.flush()
    pwShort.close()
    println("grepTextStart--------------------------------------")
    outPutResultPrinter.printResultsList(outPutResults)
    println("grepTextEnd--------------------------------------")

    println(outPutResultPrinter.printResults(outPutResults,true))
  }

  def visulizeCluster(setting: Settings, chs: ((Tile, Int), (Tile, Int)), first : Boolean): Unit = {
    val image = new TileVisualizer()
    if(first){
      image.visualTileNew(chs._1._1, setting, "cluster")
    }
    image.visualTileNew(chs._2._1, setting, "cluster")
  }

  def visulizeCluster(setting: Settings, clusters: Tile): Unit = {
    val image = new TileVisualizer()
    image.visualTileNew(clusters, setting, "cluster")
  }

  def gStar(tile : Tile, settings : parmeters.Settings, visualize : Boolean): Tile = {
    GenericScenario.gStar(tile,settings,visualize)
  }

  def getRaster(settings : Settings): Tile = {
    val serializer = new SerializeTile(settings.serilizeDirectory)
    var raster : Tile = null
    if(!settings.fromFile){
      raster = creatRaster(settings)
      serializer.write(raster)
      settings.fromFile = false
    } else {
      raster = serializer.read()
    }
    aggregateToZoom(raster, settings.aggregationLevel)
  }

  def aggregateToZoom(tile : Tile, zoomLevel : Int) : Tile = {
    val result : Tile = tile.downsample(tile.cols/zoomLevel, tile.rows/zoomLevel)(f =>
    {var sum = 0
      f.foreach((x:Int,y:Int)=>if(x<tile.cols && y<tile.rows) sum+=tile.get(x,y) else sum+=0)
      sum}
    )
    result
  }

  def aggregateTile(tile : Tile): Tile ={
    val result : Tile = tile.downsample(tile.cols/2, tile.rows/2)(f =>
    {var sum = 0
      f.foreach((x:Int,y:Int)=>if(x<tile.cols && y<tile.rows) sum+=tile.get(x,y) else sum+=0)
      sum}
    )
    result
  }

  def creatRaster(settings : Settings): Tile = {
    var startTime = System.currentTimeMillis()
    val transform = new Transformation
    val arrayTile = transform.transformCSVtoRaster(settings)
    //arrayTile.histogram.values().map(x => println(x))
    println("Time for RasterTransformation =" + ((System.currentTimeMillis() - startTime) / 1000))
    println("Raster Size (cols,rows)=(" + arrayTile.cols + "," + arrayTile.rows + ")")
    arrayTile
  }

  def creatMulitRaster(settings : Settings): MultibandTile = {
    var startTime = System.currentTimeMillis()
    val transform = new Transformation
    val multibandTile = transform.transformCSVtoTimeRaster(settings)
    //arrayTile.histogram.values().map(x => println(x))
    println("Time for RasterTransformation =" + ((System.currentTimeMillis() - startTime) / 1000))
    println("Raster Size (cols,rows)=(" + multibandTile.cols + "," + multibandTile.rows + ")")
    for(tile <- multibandTile.bands){
      aggregateToZoom(tile, settings.aggregationLevel)
    }
    multibandTile
  }

  def forGlobalG(globalSettings: Settings, outPutResults: ListBuffer[SoHResult], runs: Int): Unit

  def forFocalG(globalSettings: Settings, outPutResults: ListBuffer[SoHResult], runs: Int): Unit

  def oneCase(globalSettings: Settings, i : Int, runs : Int): (Settings, ((Tile, Int), (Tile, Int)), SoHR, (Int, Int))

  def getRasterWithCorrectResolution(globalSettings: Settings, i : Int, runs : Int, next : Int): (Tile,Int,Int) = {
    val actualLat = ((globalSettings.latMax - globalSettings.latMin) / (10.0 + 990.0 / runs.toDouble * i + next)).ceil.toInt
    val actualLon = ((globalSettings.lonMax - globalSettings.lonMin) / (10.0 + 990.0 / runs.toDouble * i + next)).ceil.toInt
    globalSettings.sizeOfRasterLat = actualLat
    globalSettings.sizeOfRasterLon = actualLon
    var raster_plus1 = getRaster(globalSettings)
    if (!globalSettings.fromFile) {
      raster_plus1 = raster_plus1.resample(actualLat, actualLon)
      println("Raster Size (cols,rows)=(" + raster_plus1.cols + "," + raster_plus1.rows + ")")
    }
    (raster_plus1,(10.0 + 990.0 / runs.toDouble * i + next).ceil.toInt,(10.0 + 990.0 / runs.toDouble * i + next).ceil.toInt)
  }

  def getMulitRasterWithCorrectResolution(globalSettings: Settings, i : Int, runs : Int, next : Int): (MultibandTile,Int,Int) = {
    val actualLat = ((globalSettings.latMax - globalSettings.latMin) / (10.0 + 990.0 / runs.toDouble * i + next)).ceil.toInt
    val actualLon = ((globalSettings.lonMax - globalSettings.lonMin) / (10.0 + 990.0 / runs.toDouble * i + next)).ceil.toInt
    globalSettings.sizeOfRasterLat = actualLat
    globalSettings.sizeOfRasterLon = actualLon
    var mulitband = creatMulitRaster(globalSettings)
    if (!globalSettings.fromFile) {
        mulitband = mulitband.resample(actualLat, actualLon)
        println("Raster Size (cols,rows)=(" + mulitband.cols + "," + mulitband.rows + ")")
    }
    (mulitband,(10.0 + 990.0 / runs.toDouble * i + next).ceil.toInt,(10.0 + 990.0 / runs.toDouble * i + next).ceil.toInt)
  }

  def getRasterFromGeoTiff(globalSettings : Settings, extra : String, tileFunction :  => Tile): Tile = {
    val importer = new ImportGeoTiff()
    if (!importer.geoTiffExists(globalSettings, extra)) {
      val tile = tileFunction
      importer.writeGeoTiff(tile, globalSettings, extra)
    }
    return importer.getGeoTiff(globalSettings, extra)
  }

  def getRasterFromMulitGeoTiff(globalSettings : Settings, i : Int, runs : Int, next : Int, extra : String, tileFunction :  => MultibandTile): MultibandTile = {
    val importer = new ImportGeoTiff()
    if (globalSettings.fromFile && importer.geoTiffExists(globalSettings, extra)) {
      return importer.getMulitGeoTiff(globalSettings, extra)
    } else {
      val tile = tileFunction
      importer.writeMultiGeoTiff(tile, globalSettings, extra)

      return tile
    }
  }



}

object GenericScenario{
  def gStar(tile : Tile, settings : parmeters.Settings, visualize : Boolean): Tile = {
    var startTime = System.currentTimeMillis()
    var ord : GetisOrd = null
    if(settings.focal){
      ord = new GetisOrdFocal(tile, settings)
    } else {
      ord = new GetisOrd(tile, settings)
    }
    println("Time for G* values =" + ((System.currentTimeMillis() - startTime) / 1000))
    startTime = System.currentTimeMillis()
    val score =ord.gStarComplete()
    println("Time for G* =" + ((System.currentTimeMillis() - startTime) / 1000))

    if(visualize){
      startTime = System.currentTimeMillis()
      val image = new TileVisualizer()
      image.visualTileNew(score, settings, "gStar")
      println("Time for Image G* =" + ((System.currentTimeMillis() - startTime) / 1000))
    }
    score
  }
}
