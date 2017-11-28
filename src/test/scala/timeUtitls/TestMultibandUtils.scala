package timeUtitls

import java.util.Random

import geotrellis.raster.{ArrayMultibandTile, ArrayTile, DoubleRawArrayTile, IntRawArrayTile, MultibandTile, Tile}
import org.scalatest.FunSuite
import timeUtils.MultibandUtils

/**
  * Created by marc on 12.07.17.
  */
class TestMultibandUtils extends FunSuite {

  test("merge") {
    val one = MultibandUtils.getHistogramDouble(TestMultibandUtils.getMultibandTile1())
    //    val breaks =one.quantileBreaks(100)
    assert(one.quantileBreaks(100).length == 100)
    val rand = MultibandUtils.getHistogramDouble(TestMultibandUtils.getMultibandTileRandom())
    assert(rand.quantileBreaks(100)(98) > 97 && rand.quantileBreaks(100)(98) < 99)

  }

  test("isInTile") {
    val rnd = new Random(1)
    val testTile: Array[Double] = Array.fill(10000)(rnd.nextInt(100))
    val rasterTile1: Tile = new DoubleRawArrayTile(testTile, 100, 100)
    val testTile2: Array[Double] = Array.fill(10000)(rnd.nextInt(100))
    val rasterTile2: Tile = new DoubleRawArrayTile(testTile2, 100, 100)
    val bands = Array(rasterTile1, rasterTile2)
    val multiBand: MultibandTile = new ArrayMultibandTile(bands)
    assert(MultibandUtils.isInTile(101, 100, multiBand) == false)
    assert(MultibandUtils.isInTile(100, 100, multiBand) == false)
    assert(MultibandUtils.isInTile(100, 100, multiBand) == false)
    assert(MultibandUtils.isInTile(-101, -100, multiBand) == false)
    assert(MultibandUtils.isInTile(101, -100, multiBand) == false)
    assert(MultibandUtils.isInTile(100, 50, multiBand) == false)
    assert(MultibandUtils.isInTile(50, 100, multiBand) == false)

    assert(MultibandUtils.isInTile(50, 50, multiBand) == true)
    assert(MultibandUtils.isInTile(99, 99, multiBand) == true)
  }
}
object TestMultibandUtils{
  def getCheesBoardRaster(): MultibandTile = {
    val arrayTile = Array[Int](
      0,1,0,1,0,1,
      1,0,1,0,1,0,
      0,1,0,1,0,1,
      1,0,1,0,1,0,
      0,1,0,1,0,1,
      1,0,1,0,1,0)
    val bands = new Array[Tile](1)
    bands(0) = new IntRawArrayTile(arrayTile,6,6)
    new ArrayMultibandTile(bands)
  }


  var rnd = new Random(1)

  def getMultiband(f: => ArrayTile, bandsSize: Int): MultibandTile = {
    val bands = new Array[Tile](bandsSize)
    for (i <- 0 to bandsSize - 1) {
      bands(i) = f
    }
    val multiBand: MultibandTile = new ArrayMultibandTile(bands)
    multiBand
  }

  def getMultiband(f: => ArrayTile): MultibandTile = {
    getMultiband(f, 24)
  }

  def getMultibandTupleTileRandomWithoutReset(): (MultibandTile, MultibandTile) = {
    (getMultibandTileGeneric(24, 100, 100, nextInt), getMultibandTileGeneric(24, 100, 100, nextInt))
  }

  def getMultibandTileRandomWithoutReset(): MultibandTile = {
    getMultibandTileGeneric(24, 100, 100, nextInt)
  }

  def getMultibandTileRandom(): MultibandTile = {
    getMultibandTileGenericRandom(100, 100)
  }

  def getMultibandTile1(): MultibandTile = {
    getMultibandTileGeneric(100, 100, getOne)
  }

  def getMultibandTileGeneric1(rows: Int, cols: Int): MultibandTile = {
    getMultibandTileGeneric(24, rows, cols, getOne)
  }

  def getMultibandTileGenericRandom(rows: Int, cols: Int): MultibandTile = {
    rnd = new Random(1)
    getMultibandTileGeneric(24, rows, cols, nextInt)
  }

  def getMultibandTileGeneric(rows: Int, cols: Int, f: () => Double): MultibandTile = {
    getMultibandTileGeneric(24, rows, cols, getOne)
  }

  def getMultibandTileGeneric(bands: Int, rows: Int, cols: Int, f: () => Double): MultibandTile = {
    getMultiband(newArrayTile(cols, rows, f))
  }

  def getOne(): Double = {
    1.0
  }

  def nextInt(): Double = {
    rnd.nextInt(100)
  }

  def newArrayTile(cols: Int, rows: Int, f: () => Double): ArrayTile = {
    new DoubleRawArrayTile(Array.fill(cols * rows)(f.apply()), cols, rows)
  }





  def getTestTileCluster(): ArrayTile ={
    val arrayTile = Array[Int](
      0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,5,0,0,0,0,
      0,0,2,2,0,0,0,0,0,0,0,0,0,0,5,0,0,0,0,0,
      0,0,0,0,2,0,0,4,0,0,0,0,0,5,0,0,0,0,0,0,
      0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
      0,0,0,0,0,0,0,0,0,0,0,0,6,6,0,3,3,3,3,3,
      0,0,0,0,0,0,0,0,0,0,0,0,6,6,0,0,0,3,3,3,
      0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,3,3,0,3,
      0,0,0,9,0,0,0,0,0,0,0,0,0,0,0,0,3,3,3,3,
      0,0,0,0,9,9,0,0,0,0,0,0,0,7,7,0,3,3,3,3,
      0,0,0,0,0,0,0,0,0,0,0,0,0,7,7,3,0,0,0,0,
      0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,3,0,0,0,0,
      0,0,13,13,0,0,0,0,0,0,0,0,0,0,3,0,0,0,0,0,
      0,0,0,0,13,13,0,11,0,0,0,0,0,3,0,0,0,0,0,0,
      0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
      0,0,0,0,0,0,12,0,0,0,0,0,8,8,0,0,0,0,0,0,
      0,0,0,0,0,0,0,12,0,0,0,0,8,8,0,0,0,0,0,0,
      0,0,0,0,0,0,0,0,12,0,0,0,0,0,0,0,0,0,0,0,
      0,0,0,31,0,0,0,31,0,0,0,0,0,0,0,0,0,0,0,0,
      0,0,0,0,31,31,31,0,0,0,0,0,0,10,10,0,0,0,0,0,
      0,0,0,0,0,0,0,0,0,0,0,0,0,10,10,0,0,0,0,0)
    val weightTile = new IntRawArrayTile(arrayTile, 20,20)
    weightTile
  }

  def getTestTileCluster2(): ArrayTile ={
    val arrayTile = Array[Int](
      0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
      0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
      0,0,0,0,0,0,0,5,0,0,0,0,0,0,0,0,0,0,0,0,
      0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
      0,0,0,0,0,0,0,0,0,0,0,0,3,3,0,2,2,2,2,2,
      0,0,0,0,0,0,0,0,0,0,0,0,3,3,0,0,0,2,2,2,
      0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,2,2,0,2,
      0,0,0,4,0,0,0,0,0,0,0,0,0,0,0,0,2,2,3,2,
      0,0,0,0,4,4,0,0,0,0,0,0,0,1,1,0,2,2,2,2,
      0,0,0,0,0,0,0,0,0,0,0,0,0,1,1,0,0,0,0,0,
      0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,3,0,0,0,0,
      0,0,6,6,0,0,0,0,0,0,0,0,0,0,3,0,0,0,0,0,
      0,0,0,0,6,6,0,1,0,0,0,0,0,3,0,0,0,0,0,0,
      0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
      0,0,0,0,0,0,7,0,0,0,0,0,0,0,0,0,0,0,0,0,
      0,0,0,0,0,0,0,7,0,0,0,0,0,0,0,0,0,0,0,0,
      0,0,0,0,0,0,0,0,7,0,0,0,0,0,0,0,0,0,0,0,
      0,0,0,8,0,0,0,7,0,0,0,0,0,0,0,0,0,0,0,0,
      0,0,0,0,8,0,7,0,0,0,0,0,0,9,9,0,0,0,0,0,
      0,0,0,0,0,0,0,0,0,0,0,0,0,9,9,0,0,0,0,0)
    val weightTile = new IntRawArrayTile(arrayTile, 20,20)
    weightTile
  }

}
