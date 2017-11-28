package export

import java.awt.Color

import exportstructure.TileVisualizer
import geotrellis.raster.{ArrayTile, DoubleRawArrayTile, IntRawArrayTile}
import getisOrd.GetisOrd
import org.scalatest.FunSuite
import parmeters.Settings

/**
  * Created by marc on 09.05.17.
  */
class TestTileVisualizer extends FunSuite {

  ignore("Test Image Generation"){
    val export = new TileVisualizer()
    export.visualTile(getImageMatrixPositve(), new Settings,"TestForPositve")
    export.visualTile(getImageMatrixNegativ(), new Settings,"TestForNegative")
  }

  ignore("Test Image Generation log"){
    val para = new Settings()
    val export = new TileVisualizer()
    export.visualTileNew(getImageMatrixPositve(), para, "TestForPositve")
    export.visualTileNew(getImageMatrixNegativ(), para, "TestForNegative")
    export.visualTileNew(getTestTile(), para, "TestForCluster")
  }

  test("Test logScale"){

    val export = new TileVisualizer()
    assert(export.logScale(0,-100,100).equals(new Color(0,0,0)))
    assert(export.logScale(-100,-100,100).equals(new Color(0,0,255)))
    assert(export.logScale(100,-100,100).equals(new Color(255,0,0)))
    assert(export.logScale(50,-100,100).equals(new Color(217,0,0)))
    assert(export.logScale(-50,-100,100).equals(new Color(0,0,217)))

    assert(export.logScale(0,-1,1).equals(new Color(0,0,0)))
    assert(export.logScale(-1,-1,1).equals(new Color(0,0,255)))
    assert(export.logScale(1,-1,1).equals(new Color(255,0,0)))
    assert(export.logScale(0.50,-1,1).equals(new Color(149,0,0)))
    assert(export.logScale(-0.50,-1,1).equals(new Color(0,0,149)))
  }



  def getTestTile(): ArrayTile ={
    val arrayTile = Array[Double](
      0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,3,0,0,0,0,
      0,0,-3,-3,0,0,0,0,0,0,0,0,0,0,3,0,0,0,0,0,
      0,0,0,0,-3,0,0,1,0,0,0,0,0,3,0,0,0,0,0,0,
      0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
      0,0,0,0,0,0,0,0,0,0,0,0,3,3,0,3,3,3,3,3,
      0,0,0,8,8,8,0,0,0,0,0,0,3,3,0,0,0,3,3,3,
      0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,3,3,0,3,
      0,0,0,3,0,0,0,0,0,0,0,0,0,0,0,0,3,3,3,3,
      0,0,0,0,3,3,0,0,0,0,0,0,0,1,1,0,3,3,3,3,
      0,0,0,0,0,0,0,0,2,0,0,0,0,1,1,0,0,0,0,0,
      0,0,0,-3,0,0,0,0,0,0,0,0,0,0,0,3,0,0,0,0,
      0,0,-3,-3,0,0,0,2.5,0,0,0,0,0,0,3,0,0,0,0,0,
      0,0,0,0,-3,0,0,1,0,0,0,0,0,3,0,0,0,0,0,0,
      0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
      0,0,0,9,9,9,3,0,0,0,0,0,3,3,0,0,0,0,0,0,
      0,0,0,0,0,0,0,3,0,0,0,0,3,3,0,0,0,0,0,0,
      0,0,0,0,0,0,0,0,3,0,0,0,0,0,0,0,0,0,0,0,
      0,0,0,3,0,0,0,3,0,0,0,0,0,0,0,0,0,0,0,0,
      0,0,0,0,3,3,3,0,0,0,0,0,0,1,1,0,0,0,0,0,
      0,0,0,0,0,0,0,0,0,0,0,0,0,1,1,0,0,0,0,0)
    val weightTile = new DoubleRawArrayTile(arrayTile, 20,20)
    weightTile
  }

  def getImageMatrixPositve(): ArrayTile = {
    //From R example
    val arrayTile = Array[Double](
      0.1, 0.3, 0.5, 0.3, 0.1,
      0.3, 0.8, 1.0, 0.8, 0.3,
      0.5, 1.0, 1.0, 1.0, 0.5,
      0.3, 0.8, 1.0, 0.8, 0.3,
      0.1, 0.3, 0.5, 0.3, 0.1)
    val weightTile = new DoubleRawArrayTile(arrayTile, 5,5)
    weightTile
  }

  def getImageMatrixNegativ(): ArrayTile = {
    //From R example
    val arrayTile = Array[Double](
      -1, 0.3, 0.5, 0.3, 0.1,
      0.3, 0.8, 1.0, 0.8, 0.3,
      0.5, 1.0, 1.0, 1.0, 0.5,
      0.3, 0.8, 1.0, 0.8, -0.5,
      0.1, 0.3, 0.5, 0.3, -2)
    val weightTile = new DoubleRawArrayTile(arrayTile, 5,5)
    weightTile
  }

}
