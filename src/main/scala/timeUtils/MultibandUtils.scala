package timeUtils

import geotrellis.{Cube, Spheroid, TimeNeighbourhood, Weight}
import geotrellis.raster.{DoubleRawArrayTile, IntRawArrayTile, MultibandTile, Tile}
import geotrellis.raster.histogram.Histogram
import parmeters.Settings

/**
  * Created by marc on 11.07.17.
  * Class for mulitband manipulation
  */
object MultibandUtils {

  def getHistogramDouble(mbT : MultibandTile): Histogram[Double] ={
    var histogram = mbT.band(0).histogramDouble()
    for(b <- 1 to mbT.bandCount-1){
      histogram = histogram.merge(mbT.band(b).histogramDouble())
    }
    histogram
  }

  def getHistogramInt(mbT : MultibandTile): Histogram[Int] ={
    var histogram = mbT.band(0).histogram
    for(b <- 1 to mbT.bandCount-1){
      histogram = histogram.merge(mbT.band(b).histogram)
    }
    histogram
  }

  def isInTile(c: Int, r: Int, mbT: MultibandTile): Boolean = {
    c>=0 && r>=0 && c<mbT.cols && r < mbT.rows
  }

  def isInTile(b : Int, c: Int, r: Int, mbT: MultibandTile): Boolean = {
    c>=0 && r>=0 && c<mbT.cols && r < mbT.rows && b>=0 && b<mbT.bandCount
  }

  def getEmptyMultibandArray(mbT: MultibandTile): MultibandTile = {
    val bands = mbT.bandCount
    val size = mbT.size
    var bandArray = new Array[Tile](bands)
    for (b <- 0 to bands-1) {
      bandArray(b) = new DoubleRawArrayTile(Array.fill(size)(0), mbT.cols, mbT.rows)
    }
    val multibandTile = MultibandTile.apply(bandArray)
    multibandTile
  }

  def getEmptyIntMultibandArray(mbT: MultibandTile): MultibandTile = {
    val bands = mbT.bandCount
    val size = mbT.size
    var bandArray = new Array[Tile](bands)
    for (b <- 0 to bands-1) {
      bandArray(b) = new IntRawArrayTile(Array.fill(size)(0), mbT.cols, mbT.rows)
    }
    val multibandTile = MultibandTile.apply(bandArray)
    multibandTile
  }

  def aggregateToZoom(tile : Tile, zoomLevel : Int) : Tile = {
    val result : Tile = tile.downsample(tile.cols/zoomLevel, tile.rows/zoomLevel)(f =>
    {var sum = 0
      f.foreach((x:Int,y:Int)=>if(x<tile.cols && y<tile.rows) sum+=tile.get(x,y) else sum+=0)
      sum}
    )
    result
  }

  def aggregateToZoom(mbT: MultibandTile, zoomLevel : Int) : MultibandTile = {
    mbT.mapBands((f : Int, t:Tile)=>aggregateToZoom(t,zoomLevel))
  }

  def getWeight(settings: Settings, radius : Int, radiusTime : Int): TimeNeighbourhood ={
    if(settings.weightMatrix==Weight.One){
      return new Cube(radius,radius)
    } else {
      new Spheroid(radius,radiusTime)
    }
  }


}


