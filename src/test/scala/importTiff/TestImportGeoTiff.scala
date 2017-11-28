package importTiff


import java.util.Random

import geotrellis.proj4.CRS
import geotrellis.raster.resample.NearestNeighbor
import geotrellis.raster.{ArrayMultibandTile, ArrayTile, DoubleRawArrayTile, MultibandTile, Tile}
import geotrellis.spark.{SpatialKey, TileLayerMetadata}
import geotrellis.spark.tiling.FloatingLayoutScheme
import geotrellis.vector.ProjectedExtent
import importExport.ImportGeoTiff
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite
import parmeters.Settings
import geotrellis.raster.io.geotiff.{MultibandGeoTiff, SinglebandGeoTiff}
import geotrellis.spark.io.hadoop.{HadoopAttributeStore, HadoopLayerReader, HadoopLayerWriter, HadoopSparkContextMethodsWrapper}
import geotrellis.spark.tiling.FloatingLayoutScheme
import geotrellis.spark.{LayerId, SpatialKey, TileLayerMetadata, withProjectedExtentTilerKeyMethods, withTileRDDReprojectMethods, withTilerMethods, _}

import scalaz.stream.nio.file

/**
  * Created by marc on 05.06.17.
  */
class TestImportGeoTiff extends FunSuite {
  ignore("Test Import/Export"){
    val im = new ImportGeoTiff()

    val par = new Settings()
    var fileName = im.getFileName(par, "Test")
    println(fileName)
    im.writeGeoTiff(getTestTile(), par, fileName)
    //fileName = im.getFileName(par,0,20,"T")
    println(fileName)
    val tile = im.getGeoTiff(fileName)
    println(tile.asciiDrawDouble())
  }

  def getTestTile(): ArrayTile ={
    val arrayTile = Array[Double](
      0,0,0,1,0,0,0,
      0,1,1,1,1,1,0,
      0,1,1,1,1,1,0,
      1,1,1,1,1,1,1,
      0,1,1,1,1,1,0,
      0,1,1,1,1,1,0,
      0,0,0,1,0,0,0
    )
    val weightTile = new DoubleRawArrayTile(arrayTile, 7,7)
    weightTile
  }

  ignore("test Partitioning"){
    val im = new ImportGeoTiff()
    val crs = CRS.fromName("EPSG:3857")
    val par = new Settings()
    par.test = true
    var fileName = im.getFileName(par, "Test")
    println(fileName)
    val setting = new Settings
    val rnd = new Random(1)


    setting.layoutTileSize = (2,2)
    im.writeGeoTiff(getTestTilePartition(), par, fileName)
    val tiled = im.repartitionFiles(fileName, setting)
    assert(tiled.keys.count()==4)
    val r = tiled.map(x=>{
      println(x._1.toString)
      val sum = x._2.band(0).toArrayDouble().reduce(_+_)
      (x._1,sum)
    }).collect()
    r.map(x=>{
      if(x._1._1==0 && x._1._2==0){
        assert(x._2==0)
      }
      if(x._1._1==1 && x._1._2==0){
        assert(x._2==4*1)
      }
      if(x._1._1==0 && x._1._2==1){
        assert(x._2==4*2)
      }
      if(x._1._1==1 && x._1._2==1){
        assert(x._2==4*3)
      }
    })


  }

  def getTestTilePartition(): ArrayTile ={
    val arrayTile = Array[Double](
      0,0,1,1,
      0,0,1,1,
      2,2,3,3,
      2,2,3,3

    )
    val weightTile = new DoubleRawArrayTile(arrayTile, 4,4)
    weightTile
  }


}