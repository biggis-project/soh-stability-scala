package scripts

import clustering.{ClusterHotSpots, ClusterHotSpotsTime, ClusterRelations}
import datastructure.{GStarClusterNeighbours, PartResult, ResultTuple}
import geotrellis.Weight
import geotrellis.raster.{MultibandTile, Tile}
import geotrellis.spark.{Metadata, SpatialKey, TileLayerMetadata}
import getisOrd.SoH.ResultsSpe
import getisOrd.{SoH, TimeGetisOrd}
import importExport.{PathFormatter, _}
import org.apache.spark.rdd.RDD
import parmeters.{Scenario, Settings}
import rasterTransformation.Transformation
import scenarios.GenericScenario


/**
  * Created by marc on 19.07.17.
  */
object MetrikValidation {

  def defaultSetting(): Settings ={
    getBasicSettings(10,1,30,2,4,1,1)
  }

  def getAdd(zoom: Int,start : Int, fak : Int): Int = {
    if(zoom==2){
      return start
    } else {
      return getAdd(zoom-1,start+fak/10,fak/10)
    }
  }

  def getBasicSettings(weight : Int, weightTime : Int, focalWeight : Int, focalTime : Int, aggregationLevel : Int, month : Int, zoom : Int): Settings = {
    val settings = new Settings()
    var multiToInt = 1000000
    var add = 0.0
    if(zoom!=1){
      //add = (getAdd(zoom,72000,72000)/2)/multiToInt.toDouble
      if(zoom==2){
        add = 4000/multiToInt.toDouble
      } else if(zoom==3) {
        add = 16000 / multiToInt.toDouble
      } else if(zoom==4){
          add = 32000/multiToInt.toDouble
      } else {
        println("Zoom="+zoom)
        println("Not implemented")
        assert(false)
      }

    }

    var buttom = (40.699607+add, -74.020265 + add)
    var top = (40.769239 + 0.010368 - add, -73.948286 + 0.008021 -add)
    settings.buttom = buttom
    settings.top = top
    settings.scenario = Scenario.Time.toString
    settings.shiftToPostive = -1 * buttom._2 * multiToInt
    settings.latMin = buttom._1 * multiToInt
    settings.lonMin = buttom._2 * multiToInt + settings.shiftToPostive
    settings.latMax = top._1 * multiToInt
    settings.lonMax = top._2 * multiToInt + settings.shiftToPostive
    settings.zoomlevel = zoom
    settings.aggregationLevel = aggregationLevel
    settings.sizeOfRasterLat = Math.pow(2.toDouble,settings.aggregationLevel.toDouble-1).toInt * 50 //meters
    settings.sizeOfRasterLon = Math.pow(2.toDouble,settings.aggregationLevel.toDouble-1).toInt * 50 //meters
    settings.rasterLatLength = ((settings.latMax - settings.latMin) / settings.sizeOfRasterLat).ceil.toInt
    settings.rasterLonLength = ((settings.lonMax - settings.lonMin) / settings.sizeOfRasterLon).ceil.toInt
    settings.layoutTileSize = ((settings.rasterLatLength)/2.ceil.toInt,(settings.rasterLonLength)/2.ceil.toInt)
    settings.weightRadius = weight
    settings.weightRadiusTime = weightTime

    settings.focal = false
    settings.focalRange = focalWeight
    settings.focalRangeTime = focalTime
    settings.csvMonth = month
    settings.csvYear = 2016
    settings
  }

  def main(args: Array[String]): Unit = {
//    val downloader = new DownloadFilesFromWeb()
//    val setting = new Settings()
//    setting.csvMonth = 1
//    setting.csvYear = 2016
//    downloader.downloadNewYorkTaxiFiles(setting)

    val validation = new MetrikValidation()
    val experiments = getScenarioSettings
    for(setting <- experiments){
      println("Start of experiment:"+setting.toString)
      validation.oneTestRun(setting)
    }

  }

  def getScenarioSettings(): Array[Settings] = {
    val monthToTest = 3 //1 to 3
    val weightToTest = 2
    val weightStepSize = 1 //10 to 10+2*weightToTest
    val focalRangeToTest = 2
    val focalRangeStepSize = 5 //30 to 30+2*focalRangeToTest
    val timeDimensionStep = 2
    val aggregationSteps = 2 //400, 800
    val zoom = 3
    val experiments = new Array[Settings](monthToTest * weightToTest * focalRangeToTest * timeDimensionStep * timeDimensionStep * aggregationSteps +
                                          monthToTest * weightToTest * focalRangeToTest * timeDimensionStep * timeDimensionStep * zoom)

    var counter = 0



    for (m <- 2 to monthToTest) {
    var m = 1
      for (w <- 0 to weightToTest - 1) {
        for (f <- 0 to focalRangeToTest - 1) {
          for (tf <- 0 to timeDimensionStep - 1) {
            for (tw <- 0 to timeDimensionStep - 1) {
              for (a <- 0 to aggregationSteps - 1)  {
                  experiments(counter) = getBasicSettings(5 + w * weightStepSize, 1 + tw, 10 + f * focalRangeStepSize, 2 + tf, 3 + a, m, 1)
                  val settings = experiments(counter)
                    counter += 1
              }
              for (z <- 2 to zoom) {
               experiments(counter) = getBasicSettings(5 + w * weightStepSize, 1 + tw, 10 + f * focalRangeStepSize, 2 + tf, 3, m, z)
               val settings = experiments(counter)
                counter += 1
              }
            }
          }
        }
      }
    }

    experiments
  }
}
class MetrikValidation {
  var settings = new Settings
  val importTer = new ImportGeoTiff()
  def oneTestRun(secenarioSettings: Settings): Unit = {
    settings = secenarioSettings
    writeBand()

    val origin = importTer.getMulitGeoTiff(settings, TifType.Raw)
    //assert(origin.cols % 4 == 0 && origin.rows % 4 == 0)
    //settings.layoutTileSize = ((origin.cols / 4.0).floor.toInt, (origin.rows / 4.0).floor.toInt)
    val rdd = importTer.repartitionFiles(settings)
    //(new ImportGeoTiff().writeMultiTimeGeoTiffToSingle(origin,settings,dir+"raster.tif"))
    //----------------------------------GStar----------------------------------
    settings.focal = false
    writeOrGetGStar(rdd, origin)
    //----------------------------------GStar-End---------------------------------
    println("deb1")
    //---------------------------------Calculate Metrik----------------------------------
    if(true || !StringWriter.exists(ResultType.Metrik, settings)) {
      val metrik = writeExtraMetrikRasters(origin, rdd)
      println("---------------------------------------0ö-------------------------")
      println(metrik.toString)
      println("---------------------------------------1ö-------------------------")
      StringWriter.writeFile(metrik.toString, ResultType.Metrik, settings)
    }
    //---------------------------------Calculate Metrik-End---------------------------------
    println("deb2")
    //---------------------------------Validate-Focal-GStar----------------------------------
    validate()
    //---------------------------------Validate-Focal-GStar----------------------------------
    println("deb3")
    //---------------------------------Cluster-GStar----------------------------------
    //clusterHotspots(settings, dir, importTer)
    //---------------------------------Cluster-GStar-End---------------------------------
    //---------------------------------Focal-GStar----------------------------------
    settings.focal = true
    writeOrGetGStar(rdd, origin)
    //---------------------------------Focal-GStar-End---------------------------------
    println("deb4")
    //---------------------------------Calculate Metrik----------------------------------
    if(true || !StringWriter.exists(ResultType.Metrik, settings)) {
      val metrik = writeExtraMetrikRasters(origin, rdd)
      println("---------------------------------------2ö-------------------------")
      println(metrik.toString)
      println("---------------------------------------3ö-------------------------")
      StringWriter.writeFile(metrik.toString, ResultType.Metrik, settings)
    }
    //---------------------------------Calculate Metrik-End---------------------------------
    //---------------------------------Cluster-Focal-GStar----------------------------------
    //clusterHotspots(settings, dir, importTer)
    //---------------------------------Cluster-Focal-GStar-End---------------------------------
    println("deb5")
    //---------------------------------Validate-Focal-GStar----------------------------------
    validate()
    //---------------------------------Validate-Focal-GStar----------------------------------
  }

  def validate(): Unit = {
    println("deb.007")
    if(StringWriter.exists(ResultType.Validation,settings)){
      return
    }
    val gStar = importTer.getMulitGeoTiff(settings, TifType.GStar)
    val mbT = clusterHotspots()
    val relation = new ClusterRelations()
    val array = new Array[Double](5)
    val old = settings.csvYear
    settings.csvYear = 2011
    for (i <- 0 to 4) {
      println("deb.008, year:"+settings.csvYear)
      var compare: MultibandTile = null
      if (PathFormatter.exist(settings, TifType.Cluster)) {
        compare = importTer.getMulitGeoTiff(settings, TifType.Cluster)
      } else {
        writeBand()
        val origin = importTer.getMulitGeoTiff(settings, TifType.Raw)
        val rdd = importTer.repartitionFiles(settings)
        val gStarCompare = TimeGetisOrd.getGetisOrd(rdd, settings, origin)
        importTer.writeMultiGeoTiff(gStarCompare, settings, TifType.GStar)
        compare = (new ClusterHotSpotsTime(gStarCompare)).findClusters()
        importTer.writeMultiGeoTiff(compare, settings, TifType.Cluster)
      }
      settings.csvYear += 1

      array(i) = relation.getPercentualFitting(mbT, compare)
    }
    var res = ""
    array.map(x => res+=x+"\n")
    StringWriter.writeFile(res,ResultType.Validation,settings)
    settings.csvYear = old
  }

  def writeOrGetGStar(rdd: RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]], origin: MultibandTile): ResultTuple = {
    var gStar : MultibandTile = null
    var cluster : MultibandTile = null
    println("deb.001")
    if (PathFormatter.exist(settings, TifType.GStar) ) {
      gStar = importTer.getMulitGeoTiff(settings, TifType.GStar)
    } else {
      gStar = TimeGetisOrd.getGetisOrd(rdd, settings, origin)
      importTer.writeMultiGeoTiff(gStar, settings, TifType.GStar)
    }
    println("deb.002")
    if(PathFormatter.exist(settings, TifType.Cluster)){
      cluster = importTer.getMulitGeoTiff(settings, TifType.Cluster)
    } else {
      cluster = (new ClusterHotSpotsTime(gStar).findClusters())
      importTer.writeMultiGeoTiff(cluster, settings, TifType.Cluster)
    }
    return new ResultTuple(gStar,cluster)
  }

  def getNeighbours(settings: Settings,
                    importTer: ImportGeoTiff,
                    origin: MultibandTile,
                    rdd: RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]]
                   ): GStarClusterNeighbours = {

    var focalP: ResultTuple = null
    var focalN: ResultTuple = null

    var weightP: ResultTuple = null
    var weightN: ResultTuple = null

    var aggregateP: ResultTuple = null
    var aggregateN: ResultTuple = null

    //----------------------------Focal--P------------------------
    println("deb.01")
    settings.focalRange += 1
    focalP = writeOrGetGStar(rdd, origin)
    //----------------------------Focal--N------------------------
    println("deb.02")
    settings.focalRange -= 2
    focalN = writeOrGetGStar(rdd, origin)
    settings.focalRange += 1

    //----------------------------Weight-P-------------------------
    println("deb.03")
    settings.weightRadius += 1
    weightP = writeOrGetGStar(rdd, origin)
    //----------------------------Weight-N-------------------------
    println("deb.04")
    settings.weightRadius -= 2
    weightN = writeOrGetGStar(rdd, origin)
    settings.weightRadius += 1

    //----------------------------Aggregation P--------------------------
    println("deb.05")
    settings.aggregationLevel += 1
    settings.sizeOfRasterLat = settings.sizeOfRasterLat*2 //meters
    settings.sizeOfRasterLon = settings.sizeOfRasterLon*2 //meters
    settings.rasterLatLength = ((settings.latMax - settings.latMin) / settings.sizeOfRasterLat).ceil.toInt
    settings.rasterLonLength = ((settings.lonMax - settings.lonMin) / settings.sizeOfRasterLon).ceil.toInt
    settings.layoutTileSize = ((settings.rasterLatLength)/2.ceil.toInt,(settings.rasterLonLength)/2.ceil.toInt)
    var gStarP : MultibandTile = null
    var clusterP : MultibandTile = null
    if (!PathFormatter.exist(settings, TifType.GStar)) {
      writeBand()
      val a2 = importTer.getMulitGeoTiff(settings, TifType.Raw)
      val rdd2 = importTer.repartitionFiles(settings)
      gStarP = TimeGetisOrd.getGetisOrd(rdd2, settings, a2)
      importTer.writeMultiGeoTiff(gStarP, settings, TifType.GStar)
    }
    if(!PathFormatter.exist(settings, TifType.Cluster)){
      gStarP = importTer.getMulitGeoTiff(settings, TifType.GStar)
      clusterP = (new ClusterHotSpotsTime(gStarP).findClusters())
      importTer.writeMultiGeoTiff(clusterP, settings, TifType.Cluster)
      aggregateP = new ResultTuple(gStarP,clusterP)
    } else {
      aggregateP = writeOrGetGStar(rdd, origin)
    }
    //----------------------------Aggregation N--------------------------
    println("deb.06")
    settings.aggregationLevel -= 2
    settings.sizeOfRasterLat = settings.sizeOfRasterLat/4 //meters
    settings.sizeOfRasterLon = settings.sizeOfRasterLon/4 //meters
    settings.rasterLatLength = ((settings.latMax - settings.latMin) / settings.sizeOfRasterLat).ceil.toInt
    settings.rasterLonLength = ((settings.lonMax - settings.lonMin) / settings.sizeOfRasterLon).ceil.toInt
    settings.layoutTileSize = ((settings.rasterLatLength)/2.ceil.toInt,(settings.rasterLonLength)/2.ceil.toInt)
    var gStarN : MultibandTile = null
    var clusterN : MultibandTile = null
    if (!PathFormatter.exist(settings, TifType.GStar)) {
      writeBand()
      val a2 = importTer.getMulitGeoTiff(settings, TifType.Raw)
      val rdd2 = importTer.repartitionFiles(settings)
      gStarN = TimeGetisOrd.getGetisOrd(rdd2, settings, a2)
      importTer.writeMultiGeoTiff(gStarN, settings, TifType.GStar)
    }
    if(!PathFormatter.exist(settings, TifType.Cluster)){
      gStarN = importTer.getMulitGeoTiff(settings, TifType.GStar)
      clusterN = (new ClusterHotSpotsTime(gStarN).findClusters())
      importTer.writeMultiGeoTiff(clusterN, settings, TifType.Cluster)
      aggregateN = new ResultTuple(gStarN, clusterN)
    } else {
      aggregateN = writeOrGetGStar(rdd, origin)
    }
    settings.aggregationLevel += 1
    settings.sizeOfRasterLat = 2*settings.sizeOfRasterLat //meters
    settings.sizeOfRasterLon = 2*settings.sizeOfRasterLon //meters
    settings.rasterLatLength = ((settings.latMax - settings.latMin) / settings.sizeOfRasterLat).ceil.toInt
    settings.rasterLonLength = ((settings.lonMax - settings.lonMin) / settings.sizeOfRasterLon).ceil.toInt
    settings.layoutTileSize = ((settings.rasterLatLength)/2.ceil.toInt,(settings.rasterLonLength)/2.ceil.toInt)
    new GStarClusterNeighbours(new PartResult(focalP, focalN), new PartResult(weightP, weightN), new PartResult(aggregateP, aggregateN))
  }

  def getMonthTile(): Tile = {
    println("deb.006")
    var gStar : Tile = null
    var cluster : Tile = null
    if (!PathFormatter.exist(settings, TifType.GStar,false)) {
      val transform = new Transformation
      val arrayTile = transform.transformCSVtoRaster(settings)
      gStar = GenericScenario.gStar(arrayTile, settings, false)
      importTer.writeGeoTiff(gStar, settings, TifType.GStar)
    } else {
      gStar = importTer.readGeoTiff(settings, TifType.GStar)
    }
    if(!PathFormatter.exist(settings, TifType.Cluster,false)){
      cluster = (new ClusterHotSpots(gStar)).findClusters()
      importTer.writeGeoTiff(cluster, settings, TifType.Cluster)
    } else {
      cluster = importTer.readGeoTiff(settings, TifType.Cluster)
    }
    cluster
  }

  def getMonthTileGisCup(origin: MultibandTile, rdd: RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]]): MultibandTile = {
    println("deb.005")
    val timeOld = settings.weightRadiusTime
    val radiusOld = settings.weightRadius
    val focalOld = settings.focal
    val focalRadius = settings.focalRange
    val focalTime = settings.focalRangeTime
    settings.focal = false
    settings.focalRange = 1
    settings.focalRangeTime = 1
    settings.weightRadiusTime = 1
    settings.weightRadius = 1
    settings.weightMatrix = Weight.One
    var res : MultibandTile = null
    if (PathFormatter.exist(settings, TifType.GStar)) {
      res = importTer.getMulitGeoTiff(settings, TifType.GStar)
    } else {
      res = TimeGetisOrd.getGetisOrd(rdd, settings, origin)
      importTer.writeMultiGeoTiff(res, settings, TifType.GStar)
    }
    settings.weightRadiusTime = timeOld
    settings.weightRadius = radiusOld
    settings.weightMatrix = Weight.Square
    settings.focal = focalOld
    settings.focalRange = focalRadius
    settings.focalRangeTime = focalTime
    res
  }

  def writeExtraMetrikRasters(origin: MultibandTile, rdd: RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]]) : ResultsSpe = {
//    val neighbours = getNeighbours(settings: Settings, importTer: ImportGeoTiff, origin, rdd)
    val gStar = importTer.getMulitGeoTiff(settings, TifType.GStar)
    settings.weightRadius += 1
    val old = writeOrGetGStar(rdd, origin)
    settings.weightRadius -= 1
//    val clusterNeighbours = neighbours.getClusterNeighbours()
    val month: Tile = getMonthTile()
//    val gisCups = getMonthTileGisCup(origin,rdd)
    val cluster = clusterHotspots()
//    StringWriter.writeFile(SoH.getPoints(cluster,settings),ResultType.HotSpots,settings)
    //jaccardPercent : Double, jaccardPercentTime : Double , jaccard : Double, jaccardTime : Double, sohTime : Double
    new ResultsSpe(SoH.getJaccardIndexPercent(cluster,old.getCluster()),
                  SoH.getJaccardIndexPercent(cluster,month),
                  SoH.getJaccardIndex(cluster,old.getCluster()),
                  SoH.getJaccardIndex(cluster,month),
                  SoH.getSoHDowAndUp(cluster,old.getCluster()).getDown())
//      getMetrikResults(gStar,
//      null, //cluster,
//      null, //clusterNeighbours._3,
//      null, //clusterNeighbours._2,
//      null, //clusterNeighbours._1,
//      neighbours.gStar._2._2,
//      month,
//      settings,
//      (new ClusterHotSpotsTime(gisCups)).findClusters())
  }

  def clusterHotspots(): MultibandTile = {
    println("deb.003")
    if (PathFormatter.exist(settings, TifType.Cluster)) {
      importTer.getMulitGeoTiff(settings, TifType.Cluster)
    }
    val gStar = importTer.getMulitGeoTiff(settings, TifType.GStar)
    var clusterHotSpotsTime = new ClusterHotSpotsTime(gStar)
    val hotSpots = clusterHotSpotsTime.findClusters()
    (new ImportGeoTiff().writeMultiGeoTiff(hotSpots, settings, TifType.Cluster))
    return hotSpots
  }

  def writeBand(): Unit = {
    println("deb.004")
    if (PathFormatter.exist(settings, TifType.Raw)) {
      return
    }
    val transform = new Transformation()
    val mulitBand = transform.transformCSVtoTimeRaster(settings)
    importTer.writeMultiGeoTiff(mulitBand, settings, TifType.Raw)
  }

}
