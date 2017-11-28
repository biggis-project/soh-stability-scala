package clustering


import geotrellis.raster.resample.NearestNeighbor
import geotrellis.raster.{DoubleRawArrayTile, IntArrayTile, IntRawArrayTile, MultibandTile, Tile}

/**
  * Created by marc on 12.05.17.
  * Used for SoH to get intersection and union of images.
  * Also for scaling of two tiles to the same size and
  * for percentual cover between two tiles
  */
class ClusterRelations {

  def getNumberChildrenAndParentsWhichIntersect(parentTile : Tile, childTile : Tile): (Int, Int) ={
    val scaled = rescaleBiggerTile(parentTile,childTile)
    //assert(parentTile.cols==childTile.cols&& childTile.rows==parentTile.rows)
    val parent = scaled._1
    val child = scaled._2
    var result = scala.collection.mutable.Set[(Int, Int)]()
    var childSet = scala.collection.mutable.Set[Int]()
    var parentSet = scala.collection.mutable.Set[Int]()
    //assert(parent.cols==child.cols && parent.rows==child.rows)
    for(i <- 0 to parent.cols-1){
      for(j <- 0 to parent.rows-1) {
        if (i < child.cols && j < child.rows) {
          if (child.get(i, j) != 0 && parent.get(i, j) != 0) {
            result += ((child.get(i, j), parent.get(i, j)))
          }
          if (parent.get(i, j) == 0 && child.get(i, j) != 0) {
            childSet += child.get(i, j)
          }
        }
      }
     }
    for((child, parent) <- result){
      //childSet += child
      parentSet += parent
    }
    //Child ohne Parent //Schnittmenge
    (childSet.size, parentSet.size)
  }



  def getNumberChildrenAndParentsWhichIntersect(parentTile : MultibandTile, childTile : MultibandTile): (Int, Int) ={
    val scaled = rescaleBiggerTile(parentTile,childTile)

    val parent = scaled._1
    val child = scaled._2
    var result = scala.collection.mutable.Set[(Int, Int)]()
    var childSet = scala.collection.mutable.Set[Int]()
    var parentSet = scala.collection.mutable.Set[Int]()
    //assert(parent.cols==child.cols && parent.rows==child.rows)
    for(b <- 0 to parent.bandCount-1) {
      for (i <- 0 to parent.cols - 1) {
        for (j <- 0 to parent.rows - 1) {
          if (i < child.cols && j < child.rows) {
            if (child.band(b).get(i, j) != 0 && parent.band(b).get(i, j) != 0) {
              result += ((child.band(b).get(i, j), parent.band(b).get(i, j)))
            }
            if (parent.band(b).get(i, j) == 0 && child.band(b).get(i, j) != 0) {
              childSet += child.band(b).get(i, j)
            }
          }
        }
      }
    }
    for((child, parent) <- result){
      //childSet += child
      parentSet += parent
    }
    //Child ohne Parent //Schnittmenge
    (childSet.size, parentSet.size)
  }

  def rescaleBiggerTile(parent : MultibandTile, child : MultibandTile): (MultibandTile,MultibandTile) ={
    if(parent.rows>child.rows){
      if(parent.cols>child.cols){
        return (parent,child.resample(parent.cols, parent.rows))
      } else if(parent.cols<child.cols){
        return (parent.resample(child.cols,parent.rows), child.resample(child.cols, parent.rows))
      }
    } else if(parent.rows<child.rows) {
      if(parent.cols>child.cols){
        return (parent.resample(parent.cols,child.rows), child.resample(parent.cols,child.rows))
      } else if(parent.cols<child.cols){
        return (parent.resample(child.cols, child.rows), child)
      }
    }
    return (parent, child)
  }





  def rescaleBiggerTile(parent : Tile, child : Tile): (Tile,Tile) ={
    if(parent.rows>child.rows){
      if(parent.cols>child.cols){
        return (parent,child.resample(parent.cols, parent.rows))
      } else if(parent.cols<child.cols){
        return (parent.resample(child.cols,parent.rows), child.resample(child.cols, parent.rows))
      }
    } else if(parent.rows<child.rows) {
      if(parent.cols>child.cols){
        return (parent.resample(parent.cols,child.rows), child.resample(parent.cols,child.rows))
      } else if(parent.cols<child.cols){
        return (parent.resample(child.cols, child.rows), child)
      }
    }
    return (parent, child)
  }

  def getPercentualFitting(origionCluster : MultibandTile, compareCluster : MultibandTile): Double ={
    return 0.0
    var fit = 0
    var total = 0
    for(b <- 0 to origionCluster.bandCount-1){
      for(r <- 0 to origionCluster.rows-1){
        for(c <- 0 to origionCluster.cols-1){
          if(origionCluster.band(b).getDouble(c,r)!=0){
            if(compareCluster.band(b).getDouble(c,r)!=0){
              fit += 1
              total += 1
            } else {
              total += 1
            }
          }
        }
      }
    }
    fit/total.toDouble
  }

  def getPercentualFitting(origionCluster : Tile, compareCluster : Tile): Double ={
    var fit = 0
    var total = 0
      for(r <- 0 to origionCluster.rows-1){
        for(c <- 0 to origionCluster.cols-1){
          if(origionCluster.getDouble(c,r)!=0){
            if(compareCluster.getDouble(c,r)!=0){
              fit += 1
              total += 1
            } else {
              total += 1
            }
          }
        }
      }
    if(total<=0){
      return 0
    }
    fit/total.toDouble
  }

}
