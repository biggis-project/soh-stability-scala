package clustering

import geotrellis.raster.{IntArrayTile, Tile}

/**
  * Created by marc on 11.05.17.
  * Used for clustering the result of G*
  * Inspired by DBScan. Instead of minPoints trashold value is needed. Default Range is 1. Other Ranges not implemented
  */
class ClusterHotSpots(tile : Tile) {


  /**
    * Main function of this class. Used to calcualate the clusters for the SoH
    * @param range
    * @param critical
    * @return
    */
  def findClusters(range : Double, critical : Double) : (Tile,Int) ={
    //TODO if range is different then 1 regionQuery need to be fixed
    val breaks = tile.histogramDouble.quantileBreaks(100)
    var qNegativ = -critical
    var q = critical
    if(breaks.length==100){
      q = breaks(98)//Math.max(breaks(98),critical)
      qNegativ = breaks(1)//Math.min(breaks(1),-critical)
    }

    var counterCluster = 0

    var tempCluster = 0;
    var visit = IntArrayTile.fill(0,tile.cols,tile.rows)
    var clusterTile = IntArrayTile.fill(0,tile.cols,tile.rows)
    for(c <- 0 to tile.cols-1){
      for(r <- 0 to tile.rows-1){
        if((tile.getDouble(c,r))>q) {
          if (clusterTile.get(c,r) == 0) {
            counterCluster += 1
            visit.set(c,r, 1)
            clusterTile.set(c,r, counterCluster)
            expandCluster(clusterTile, 1, q, visit, counterCluster, regionQuery(1, q, c,r, visit))
          }
        }
//      Negative Case not working
//         else if((tile.getDouble(i,j))<qNegativ){
//          if(clusterTile.get(i,j)==0){
//            counterCluster += 1
//            visit.set(i,j,1)
//            clusterTile.set(i,j,-counterCluster)
//            expandClusterNegative(clusterTile, range, qNegativ, visit, -counterCluster, regionQueryNegative(range, qNegativ, i, j, visit))
//          }
//        }
      }

    }
    (clusterTile,counterCluster)
  }

  /**
    * Default values only used if there is no 1% quantil
    * @return
    */
  def findClusters() : Tile ={

    findClusters(1.9,5)._1
  }

  def findClustersTest() : Tile ={
    val breaks = tile.histogramDouble.quantileBreaks(100)
    var q = 1.96


    var counterCluster = 0

    var tempCluster = 0;
    var visit = IntArrayTile.fill(0,tile.cols,tile.rows)
    var clusterTile = IntArrayTile.fill(0,tile.cols,tile.rows)
    for(c <- 0 to tile.cols-1){
      for(r <- 0 to tile.rows-1){
        if((tile.getDouble(c,r))>q) {
          if (clusterTile.get(c,r) == 0) {
            counterCluster += 1
            visit.set(c,r, 1)
            clusterTile.set(c,r, counterCluster)
            expandCluster(clusterTile, 1, q, visit, counterCluster, regionQuery(1, q, c,r, visit))
          }
        }

      }

    }
    clusterTile
  }

  private def replaceNumber(oldNumber: Int, newNumber : Int, clusterTile: IntArrayTile) : Unit = {
    for(i <- 0 to tile.cols-1) {
      for (j <- 0 to tile.rows - 1) {
        if(clusterTile.get(i,j)==oldNumber){
          clusterTile.set(i,j, newNumber)
        }
      }
    }
  }

  private def regionQuery(range: Double, critical: Double, clusterCol: Int, clusterRow: Int, visit: IntArrayTile) : List[(Int,Int)] = {
    var neighborhood = List[(Int, Int)]()
    for (c <- -range.toInt to range.toInt) {
      for (r <- -range.toInt to range.toInt) {
        if (clusterCol + c < tile.cols && clusterCol + c >= 0
          && clusterRow + r < tile.rows && clusterRow + r >= 0
          && visit.get(clusterCol + c, clusterRow + r) == 0) {
          visit.set(clusterCol + c, clusterRow + r, 1)
          if (tile.getDouble(clusterCol + c, clusterRow + r) > critical) {
            neighborhood = (clusterCol + c, clusterRow + r) :: neighborhood
          }
        }
      }
    }
    neighborhood
  }



  private def expandCluster(clusterTile: IntArrayTile, range: Double, critical: Double, visit: IntArrayTile, counterCluster: Int, neigbourhoud : List[(Int,Int)] ) : Unit = {
    var nextNeigbours = List[(Int,Int)]()
    for((c,r) <- neigbourhoud){
      clusterTile.set(c,r,counterCluster)
      nextNeigbours = List.concat(nextNeigbours, regionQuery(range, critical, c, r, visit))
    }
    if(nextNeigbours.size>0){
      expandCluster(clusterTile, range, critical, visit, counterCluster, nextNeigbours)
    }
  }




}
