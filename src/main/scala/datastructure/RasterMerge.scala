package datastructure

import geotrellis.raster.Tile


/**
  * Created by marc on 16.05.17.
  */
class RasterMerge {


  def tileAdd(t1 : Tile, t2 : Tile): Tile = {
    assert(t1.cols==t2.cols && t2.rows==t1.rows)
    return t1.dualCombine(t2){ (z1: Int,z2: Int) =>
      z1 + z2
    } { (z1: Double, z2: Double) =>
      z1 + z2
    }
  }

//  def rasterAdd(t1 : Raster[Tile], t2 : Raster[Tile]): Raster[Tile] ={
//    assert(t1.cols==t2.cols && t2.rows==t1.rows)
//    return t1._1.dualCombine(t2._1){ (z1: Int,z2: Int) =>
//      z1 + z2
//    } { (z1: Double, z2: Double) =>
//      z1 + z2
//    }
//  }
}
