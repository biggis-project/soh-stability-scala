package clustering

import geotrellis.raster.{ArrayMultibandTile, ArrayTile, DoubleRawArrayTile, IntRawArrayTile, MultibandTile, Tile}
import org.scalatest.FunSuite

/**
  * Created by marc on 11.05.17.
  */
class TestClusterHotSpotsTime extends FunSuite{
  test("TestClusterHotSpots"){
    val bands = new Array[Tile](24)
    for(i <- 0 to 23){
      bands(i) = getTestTile()
    }
    val multiBand : MultibandTile = new ArrayMultibandTile(bands)
    val chs = new ClusterHotSpotsTime(multiBand)
    println(chs.findClusters(1.5, 2)._1.band(0).asciiDraw())
    //println((chs.findClusters(1.5, 2)._1-getTestTile()).asciiDraw())
    assert(chs.findClusters(1.5, 2)._2==8) //10 if negative included

  }

  test("TestClusterHotSpots Big"){
    val bands = new Array[Tile](24)
    for(i <- 0 to 23){
      bands(i) = new IntRawArrayTile(Array.fill(500*500)(3), 500, 500)
    }
    val multiBand : MultibandTile = new ArrayMultibandTile(bands)
    val chs = new ClusterHotSpotsTime(multiBand)
    assert(chs.findClusters(1.5, 2)._2==0)
  }


  def getTestTile(): ArrayTile ={
    val arrayTile = Array[Double](
      0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,3,0,0,0,0,
      0,0,-3,-3,0,0,0,0,0,0,0,0,0,0,3,0,0,0,0,0,
      0,0,0,0,-3,0,0,1,0,0,0,0,0,3,0,0,0,0,0,0,
      0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
      0,0,0,0,0,0,0,0,0,0,0,0,3,3,0,3,3,3,3,3,
      0,0,0,0,0,0,0,0,0,0,0,0,3,3,0,0,0,3,3,3,
      0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,3,3,0,3,
      0,0,0,3,0,0,0,0,0,0,0,0,0,0,0,0,3,3,3,3,
      0,0,0,0,3,3,0,0,0,0,0,0,0,1,1,0,3,3,3,3,
      0,0,0,0,0,0,0,0,0,0,0,0,0,1,1,0,0,0,0,0,
      0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,3,0,0,0,0,
      0,0,-3,-3,0,0,0,0,0,0,0,0,0,0,3,0,0,0,0,0,
      0,0,0,0,-3,3,0,1,0,0,0,0,0,3,0,0,0,0,0,0,
      0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
      0,0,0,0,0,0,3,0,0,0,0,0,3,3,0,0,0,0,0,0,
      0,0,0,0,0,0,0,3,0,0,0,0,3,3,0,0,0,0,0,0,
      0,0,0,0,0,0,0,0,3,0,0,0,0,0,0,0,0,0,0,0,
      0,0,0,3,0,0,0,3,0,0,0,0,0,0,0,0,0,0,0,0,
      0,0,0,0,3,3,3,0,0,0,0,0,0,1,1,0,0,0,0,0,
      0,0,0,0,0,0,0,0,0,0,0,0,0,1,1,0,0,0,0,0)
    val weightTile = new DoubleRawArrayTile(arrayTile, 20,20)
    weightTile
  }
}
