package datastructure

import geotrellis.raster.MultibandTile

/**
  * Created by marc on 14.08.17.
  */
class GStarClusterNeighbours(f : PartResult, w : PartResult, a : PartResult){
  val gStar = ((f.getPlus().getGStar(),f.getMinus().getGStar()),(w.getPlus().getGStar(),w.getMinus().getGStar()),(a.getPlus().getGStar(),a.getMinus().getGStar()))
  val cluster = ((f.getPlus().getCluster(),f.getMinus().getCluster()),(w.getPlus().getCluster(),w.getMinus().getCluster()),(a.getPlus().getCluster(),a.getMinus().getCluster()))
  def getGStarNeighbours(): ((MultibandTile, MultibandTile), (MultibandTile, MultibandTile), (MultibandTile, MultibandTile)) ={
    return gStar
  }
  def getClusterNeighbours(): ((MultibandTile, MultibandTile), (MultibandTile, MultibandTile), (MultibandTile, MultibandTile)) ={
    return cluster
  }
}


class ResultTuple(gStar : MultibandTile, cluster : MultibandTile){
  def getGStar(): MultibandTile ={
    gStar
  }
  def getCluster(): MultibandTile ={
    cluster
  }
}

class PartResult(p : ResultTuple, n : ResultTuple){
  def getPlus(): ResultTuple ={
    p
  }
  def getMinus(): ResultTuple ={
    n
  }
}
