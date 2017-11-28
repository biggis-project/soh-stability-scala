package clustering


import geotrellis.raster.{ArrayTile, DoubleRawArrayTile, IntArrayTile, IntRawArrayTile}
import org.scalatest.FunSuite

/**
  * Created by marc on 11.05.17.
  */
class TestClusterHotSpots extends FunSuite{
  test("TestClusterHotSpots"){
    val chs = new ClusterHotSpots(getTestTile())
    println(chs.findClusters(1.5, 2)._1.asciiDraw())
    //println((chs.findClusters(1.5, 2)._1-getTestTile()).asciiDraw())
    assert(chs.findClusters(1.5, 2)._2==8) //10 if negative included

  }

  test("TestClusterHotSpots Big"){
    val testTile = Array.fill(500*500)(3)
    val rasterTile = new IntRawArrayTile(testTile, 500, 500)
    val chs = new ClusterHotSpots(rasterTile)
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
