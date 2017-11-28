package clustering

import geotrellis.raster.{IntRawArrayTile, MultibandTile}
import timeUtils.MultibandUtils

/**
  * Created by marc on 07.07.17.
  * Used for clustering the result of G*
  * Same as [[ClusterHotSpots]] but for geotemporalspace
  */
class ClusterHotSpotsTime(mbT : MultibandTile) {

  def findClusters() : (MultibandTile) ={
    findClusters(1.9,5)._1
  }

  def findClusters(range : Double, critical : Double) : (MultibandTile,Int) ={
    var histogramm = mbT.band(0).histogramDouble()
    for(b <- 1 to mbT.bandCount-1){
      histogramm = histogramm.merge(mbT.band(b).histogramDouble())
    }
    val breaks = histogramm.quantileBreaks(100)
    var qNegativ = -critical
    var q = critical
    if(breaks.length==100){
      q = breaks(98)//Math.max(breaks(98),critical)
      qNegativ = breaks(1)//Math.min(breaks(1),-critical)
    }
    var counterCluster = 0

    var tempCluster = 0;

    val visit = MultibandUtils.getEmptyIntMultibandArray(mbT)
    val clusterTile = MultibandUtils.getEmptyIntMultibandArray(mbT)
    for(c <- 0 to mbT.cols-1){
      //println("Next c:"+c)
      for(r <- 0 to mbT.rows-1){
        for(b <- 0 to mbT.bandCount-1)
        if((mbT.band(b).getDouble(c,r))>q) {
          if (clusterTile.band(b).get(c, r) == 0) {
            counterCluster += 1

            (visit.band(b).asInstanceOf[IntRawArrayTile]).set(c, r, 1)
            (clusterTile.band(b).asInstanceOf[IntRawArrayTile]).set(c, r, counterCluster)
            expandCluster(clusterTile, 1, q, visit, counterCluster, regionQuery(1, q, b, c, r, visit))
          }
        }
      }

    }
    (clusterTile,counterCluster)
  }

  private def expandCluster(clusterTile: MultibandTile, range: Double, critical: Double, visit: MultibandTile, counterCluster: Int, neigbourhoud : List[(Int,Int,Int)] ) : Unit = {
    var nextNeigbours = List[(Int,Int,Int)]()
    for((z,x,y) <- neigbourhoud){
      clusterTile.band(z).asInstanceOf[IntRawArrayTile].set(x,y,counterCluster)
      nextNeigbours = List.concat(nextNeigbours, regionQuery(range, critical, z, x, y, visit))
    }
    if(nextNeigbours.size>0){
      expandCluster(clusterTile, range, critical, visit, counterCluster, nextNeigbours)
    }
  }

  private def regionQuery(range: Double, critical: Double, clusterBand : Int, clusterCol: Int, clusterRow: Int, visit: MultibandTile) : List[(Int,Int,Int)] = {
    var neighborhood = List[(Int, Int, Int)]()
    for (i <- -range.toInt to range.toInt) {
      for (j <- -range.toInt to range.toInt) {
        for(b <- -range.toInt to range.toInt) {
          if (clusterCol + j < mbT.cols && clusterCol + j >= 0
            && clusterRow + i < mbT.rows && clusterRow + i >= 0
            && clusterBand+b >= 0 && clusterBand+b < mbT.bandCount
            //&& Math.sqrt(j * j + i * i + b*b) <= range
            && visit.band(clusterBand+b).get(clusterCol + j, clusterRow + i) == 0) {
            visit.band(clusterBand+b).asInstanceOf[IntRawArrayTile].set(clusterCol + j, clusterRow + i, 1)
            if (mbT.band(clusterBand+b).getDouble(clusterCol + j, clusterRow + i) > critical) {
              neighborhood = (clusterBand+b, clusterCol + j, clusterRow + i) :: neighborhood
            }
          }
        }
      }
    }
    neighborhood
  }

}
