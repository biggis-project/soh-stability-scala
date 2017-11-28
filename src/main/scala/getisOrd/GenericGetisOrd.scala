package getisOrd

import geotrellis.raster.{ArrayTile, DoubleRawArrayTile, IntRawArrayTile, Tile}
import geotrellis.raster.mapalgebra.focal.Circle
import parmeters.Settings

/**
  * Created by marc on 22.05.17.
  */
class GenericGetisOrd extends Serializable{

  def genericGStar(lR : Tile, lW : Tile, lN: Tile, lM :Tile, lS : Tile) : Tile = {
    (lR.focalSum(Circle(lW.cols/2))-lM*lW)/(lS*((lN*getPowerOfTwoForElementsAsSum(lW)-getSummForTile(lW)*getSummForTile(lW))/(lN-1)).mapDouble(x => Math.sqrt(x)))
  }

  def getPowerOfTwoForElementsAsSum(tile : Tile): Double ={
    tile.toArrayDouble().foldLeft(0.0){(x,y)=>x+y*y}
  }

  def getSummForTile(tile: Tile): Double ={
    tile.toArrayDouble().reduce(_+_)
  }

  def createNewWeight(para : Settings) : Tile = {
    getWeightMatrixSquare(para.weightRadius/2)
  }

  def getWeightMatrixSquare(radius : Int): ArrayTile ={
    val arrayTile  = Array.ofDim[Double](radius*2+1,radius*2+1)

    for (i <- -radius to radius) {
      for (j <- -radius to radius) {
        if(Math.sqrt(i*i+j*j)<=radius) {
          arrayTile(radius + i)(radius + j) = 1.0
        } else {
          arrayTile(radius + i)(radius + j) = 0.0
        }
      }
    }
    val weightTile = new DoubleRawArrayTile(arrayTile.flatten, radius*2+1,radius*2+1)
    weightTile
  }

  def getWeightMatrixSquareSigmoid(radius : Int, constantRange: Int): ArrayTile ={
    val variableRange = radius-constantRange
    if(variableRange<0){
      throw new IllegalArgumentException
    }
    val arrayTile  = Array.ofDim[Double](radius*2+1,radius*2+1)

    for (i <- -radius to radius) {
      for (j <- -radius to radius) {
        if(Math.sqrt(i*i+j*j)<=constantRange) {
          arrayTile(radius + i)(radius + j) = 1.0
        } else {
          arrayTile(radius + i)(radius + j) = 1/(1+Math.exp(-variableRange+Math.sqrt(i*i+j*j)-constantRange))
        }
      }
    }
    val weightTile = new DoubleRawArrayTile(arrayTile.flatten, radius*2+1,radius*2+1)
    weightTile
  }

  def padding(tile : Tile, padSize : Int) : Tile = {
    val rangeLeft = padSize
    val rangeRight = tile.cols-padSize
    val rangeTop = tile.rows-padSize
    val rangeButtom = padSize
    def isInRange(x : Int, y : Int) : Boolean = {
      if(x>=rangeLeft && x<rangeTop && y<rangeRight && y>=rangeButtom){
        return true
      }
      false
    }
    tile.mapDouble((x,y,d)=>if(isInRange(x,y)) d else 0)
  }


}
