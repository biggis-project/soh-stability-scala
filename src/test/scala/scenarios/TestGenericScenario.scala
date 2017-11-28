package scenarios

import geotrellis.raster.{ArrayTile, IntRawArrayTile, MultibandTile}
import org.scalatest.FunSuite
import parmeters.Settings

import scala.util.Random


/**
  * Created by marc on 08.06.17.
  */
class TestGenericScenario extends FunSuite{

  def getTile(cols : Int, rows : Int): ArrayTile ={
    val testTile = Array.fill(rows*cols)(new Random().nextInt(100))
    val rasterTile = new IntRawArrayTile(testTile, cols, rows)
    rasterTile
  }

}
