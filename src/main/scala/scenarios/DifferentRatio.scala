package scenarios

import clustering.ClusterHotSpots
import exportstructure.SoHResult
import geotrellis.Weight
import geotrellis.raster.Tile
import getisOrd.SoH
import getisOrd.SoH.SoHR
import parmeters.{Scenario, Settings}

import scala.collection.mutable.ListBuffer

/**
  * Created by marc on 24.05.17.
  */
class DifferentRatio extends GenericScenario{

  override def runScenario(): Unit ={
    val globalSettings =new Settings()
    globalSettings.fromFile = true
    globalSettings.weightMatrix = Weight.Square
    globalSettings.weightRadius = 2
    globalSettings.scenario = Scenario.Weight.toString
    var outPutResults = ListBuffer[SoHResult]()
    val runs = 5

    forGlobalG(globalSettings, outPutResults, runs)
    saveResult(globalSettings, outPutResults)
    outPutResults = ListBuffer[SoHResult]()
    forFocalG(globalSettings, outPutResults, runs)

    saveResult(globalSettings, outPutResults)
  }

  override def forGlobalG(globalSettings: Settings, outPutResults: ListBuffer[SoHResult], runs: Int): Unit = {
    for(k <- 1 to 5) {
      globalSettings.aggregationLevel = k
      //globalSettings.weightRadius = 1+k*2
      for (i <- 0 to 9) {
        var totalTime = System.currentTimeMillis()
        globalSettings.focal = false
        globalSettings.focalRange = 0
        if (k == 0) {
          globalSettings.fromFile = false
        } else {
          globalSettings.fromFile = false
        }

          val (para: Settings, chs: ((Tile, Int), (Tile, Int)), sohVal: SoHR, lat: (Int, Int)) = oneCase(globalSettings, i, runs)
          saveSoHResults((System.currentTimeMillis() - totalTime) / 1000, outPutResults, para, chs, sohVal, lat)


      }
    }
  }

  override def forFocalG(globalSettings: Settings, outPutResults: ListBuffer[SoHResult], runs: Int): Unit = {
    for(k <- 1 to 5){
      globalSettings.aggregationLevel = k
      //globalSettings.weightRadius = 1+k*2
      for (j <- 0 to 9) {
        //globalSettings.weightRadius =3+j*2
        globalSettings.focalRange = 2+j*6
        for (i <- 0 to 9) {
          logger.info("k,j,i:"+k+","+j+","+i)
          val totalTime = System.currentTimeMillis()
          globalSettings.focal = true
          if (k == 0) {
            globalSettings.fromFile = false
          } else {
            globalSettings.fromFile = false
          }

          val w = 1+(i+1)*2
          if(w<globalSettings.focalRange){
            //globalSettings.weightRadius = weightRatio(globalSettings, runs, j)
            val (para: Settings, chs: ((Tile, Int), (Tile, Int)), sohVal: SoHR, lat: (Int, Int)) = oneCase(globalSettings, i, runs)
            saveSoHResults((System.currentTimeMillis() - totalTime) / 1000, outPutResults, para, chs, sohVal, lat)
          }
        }
      }
    }

  }

  def weightRatio(globalSettings: Settings, runs: Int, j: Int): Double = {
    (globalSettings.focalRange / runs.toDouble) * j
  }



  override def oneCase(globalSettings: Settings, i : Int, runs : Int): (Settings, ((Tile, Int), (Tile, Int)), SoHR, (Int, Int)) = {
    globalSettings.sizeOfRasterLat = 100
    globalSettings.sizeOfRasterLon = 100

    //globalSettings.zoomLevel = i
    //globalSettings.focalRange = 2+i*6
    globalSettings.weightRadius = 1+i*2
    val raster : Tile = getRasterFromGeoTiff(globalSettings, "raster", getRaster(globalSettings))
    val gStarParent = getRasterFromGeoTiff(globalSettings, "gStar", gStar(raster, globalSettings, true))
    val clusterParent = getRasterFromGeoTiff(globalSettings, "cluster",((new ClusterHotSpots(gStarParent)).findClusters(globalSettings.clusterRange, globalSettings.critivalValue))._1)
   //val clusterParent = (new ClusterHotSpots(gStarParent)).findClusters(globalSettings.clusterRange, globalSettings.critivalValue)._1

    // For Testing
    //    for(r <- 1 to clusterParent.rows-2){
//      for(c <- 1 to clusterParent.cols-2){
//        if(clusterParent.getDouble(c,r)!=0 ){
//          println("-------------------------------------")
//          val t = clusterParent.getDouble(c,r)
//          assert( 0==clusterParent.getDouble(c-1,r-1) || t==clusterParent.getDouble(c-1,r-1))
//          assert( 0==clusterParent.getDouble(c-1,r) || t==clusterParent.getDouble(c-1,r))
//          assert( 0==clusterParent.getDouble(c,r-1) || t==clusterParent.getDouble(c,r-1))
//          assert( 0==clusterParent.getDouble(c+1,r+1) || t==clusterParent.getDouble(c+1,r+1))
//          assert( 0==clusterParent.getDouble(c,r+1) || t==clusterParent.getDouble(c,r+1))
//          assert( 0==clusterParent.getDouble(c+1,r) || t==clusterParent.getDouble(c+1,r))
//          println(clusterParent.getDouble(c-1,r+1))
//          assert( 0==clusterParent.getDouble(c-1,r+1) || t==clusterParent.getDouble(c-1,r+1))
//          assert( 0==clusterParent.getDouble(c+1,r-1) || t==clusterParent.getDouble(c+1,r-1))
//          println("-------------------------------------")
//        }
//      }
//    }

    //globalSettings.zoomLevel = i+1
    //globalSettings.focalRange = 2+(i+1)*6
    globalSettings.weightRadius = 1+(i+1)*2
    val rasterParent : Tile = getRasterFromGeoTiff(globalSettings, "raster", getRaster(globalSettings))
    //globalSettings.zoomLevel = i
    val gStarChild = getRasterFromGeoTiff(globalSettings, "gStar", gStar(rasterParent, globalSettings, true))
    val clusterChild =getRasterFromGeoTiff(globalSettings, "cluster", (new ClusterHotSpots(gStarChild)).findClusters(globalSettings.clusterRange, globalSettings.critivalValue)._1)

    val numberclusterParent = clusterParent.findMinMax._2
    val numberclusterChild = clusterChild.findMinMax._2

    globalSettings.parent = true
    //globalSettings.focalRange = 3+i*6
    visulizeCluster(globalSettings, clusterParent)
    globalSettings.parent = false
    //globalSettings.focalRange = 3+(i+1)*6
    //visulizeCluster(globalSettings, clusterChild)

    val sohVal = SoH.getSoHDowAndUp((clusterParent),(clusterChild))
    (globalSettings, ((clusterParent,numberclusterParent),(clusterChild,numberclusterChild)), sohVal,
      (3+i*6, //Just lat for export
      3+(i+1)*6)) //Just lat for export
  }




}
