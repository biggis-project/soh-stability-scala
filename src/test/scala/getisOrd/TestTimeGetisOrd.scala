package getisOrd

import java.util.Random

import geotrellis.Spheroid
import geotrellis.raster.{ArrayMultibandTile, DoubleRawArrayTile, MultibandTile, Tile}
import geotrellis.spark.SpatialKey
import geotrellis.vector.Extent
import importExport.ImportGeoTiff
import org.scalatest.FunSuite
import parmeters.Settings

import scala.collection.mutable

/**
  * Created by marc on 03.07.17.
  */
class TestTimeGetisOrd extends FunSuite {




  def getSetup: (Spheroid, MultibandTile, mutable.HashMap[SpatialKey, MultibandTile], SpatialKey) = {
    val ownSettings = new Settings()
    ownSettings.focalRange = 5
    val rnd = new Random(1)
    val bands = new Array[Tile](24)
    val spheroid = new Spheroid(2, 1)
    for (i <- 0 to 23) {
      bands(i) = new DoubleRawArrayTile(Array.fill(10000)(2), 100, 100)
    }
    val multiBand: MultibandTile = new ArrayMultibandTile(bands)
    var hashMap = new mutable.HashMap[SpatialKey, MultibandTile]()
    var myKey = new SpatialKey(0, 0)
    (spheroid, multiBand, hashMap, myKey)
  }





  test("getGetisOrd") {
    val importTer = new ImportGeoTiff()
    val setting = new Settings
    setting.focal = false
    setting.test = true
    setting.layoutTileSize = (50,45)
    val rnd = new Random(1)
    val bands = new Array[Tile](24)
    for(i <- 0 to 23){
      bands(i) = new DoubleRawArrayTile(Array.fill(90*100)(rnd.nextInt(100)), 100, 90)
    }

    var multiBand : MultibandTile = new ArrayMultibandTile(bands)
    importTer.writeMultiGeoTiff(multiBand, setting, "/tmp/firstTimeBand.tif")
    var rdd = importTer.repartitionFiles("/tmp/firstTimeBand.tif", setting)
    var result = TimeGetisOrd.getGetisOrd(rdd,setting, multiBand)

    setting.focal = true
    setting.layoutTileSize = (5,6)
    for(i <- 0 to 23){
      bands(i) = new DoubleRawArrayTile(Array.fill(10*12)(rnd.nextInt(100)), 10, 12)
    }
    multiBand = new ArrayMultibandTile(bands)
    importTer.writeMultiGeoTiff(multiBand, setting, "/tmp/firstTimeBand.tif")
    rdd = importTer.repartitionFiles("/tmp/firstTimeBand.tif", setting)
    result = TimeGetisOrd.getGetisOrd(rdd,setting, multiBand)

  }



  test("polygonalSumDouble test"){
    //Histogramm is not working
    val bands = new Array[Tile](24)
    val rnd = new Random(1)
    for(i <- 0 to 23){
      bands(i) = new DoubleRawArrayTile(Array.fill(10000)(rnd.nextInt(100)), 100, 100)
    }
    val multiBand : MultibandTile = new ArrayMultibandTile(bands)
    multiBand.bands.map(x=>(x.polygonalSumDouble(new Extent(0,0,100,100), (new Extent(0,0,100,100)).toPolygon()),x.toArrayDouble().reduce(_+_))).foreach(x=>assert(x._1==x._2))
  }





}
