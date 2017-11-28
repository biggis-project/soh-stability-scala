package getisOrd

import java.util.Random

import geotrellis.raster.mapalgebra.focal.Circle
import geotrellis.raster.{ArrayTile, DoubleRawArrayTile, Tile}
import org.scalatest.FunSuite
import parmeters.Settings

/**
  * Created by marc on 24.05.17.
  */
class TestGenericGetisOrd extends FunSuite {

  test("Test Sigmoid Square"){
    val g = new GenericGetisOrd()
    println(g.getWeightMatrixSquareSigmoid(3,1).asciiDrawDouble())
  }

  test("Test Padding"){
    val g = new GenericGetisOrd()
    val rnd = new Random(1)
    val testTile : Array[Double]= Array.fill(25)(rnd.nextDouble()*10)
    val rasterTile1 = new DoubleRawArrayTile(testTile, 5, 5)
    var padded = g.padding(rasterTile1, 1)
    assert(padded.getDouble(0,0)==0 && padded.getDouble(1,1)!=0)
    padded = g.padding(rasterTile1, 2)
    assert(padded.getDouble(0,0)==0 && padded.getDouble(1,1)==0 && padded.getDouble(2,2)!=0)
  }



}
