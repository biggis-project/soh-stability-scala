package getisOrd


import geotrellis.Weight
import geotrellis.raster.mapalgebra.focal.Square
import geotrellis.raster.{ArrayTile, DoubleArrayTile, DoubleRawArrayTile, IntRawArrayTile, Tile}
import geotrellis.spark.{Metadata, SpaceTimeKey, TileLayerMetadata}
import org.apache.spark.rdd.RDD
import parmeters.Settings

/**
  * Created by marc on 27.04.17.
  */


class GetisOrd(tile : Tile, setting : Settings) extends GenericGetisOrd{
  var weight : Tile = createNewWeight(setting)
  var sumOfTile : Double = 0.0
  var sumOfWeight : Double = this.getSummForTile(weight)
  var xMean : Double = 0.0
  var standardDeviation : Double = 0.0
  var powerOfWeight : Double =  getPowerOfTwoForElementsAsSum(weight)
  var powerOfTile : Double =  getPowerOfTwoForElementsAsSum(tile)




  def gStar(layer: RDD[(SpaceTimeKey, Tile)] with Metadata[TileLayerMetadata[SpaceTimeKey]], weight : Array[Int]): Unit ={
    layer.metadata.gridBounds
    layer.count();
    //val tile = IntArrayTile(layer, 9,4)
    Square(1)
  }

  def printG_StarComplete(): Unit ={
    for(i <- 0 to tile.rows-1){
      for(j <- 0 to tile.cols-1){
        print(gStarForTile((i,j)))
        print(";")
      }
      println("")
    }

  }

  def calculateStats(index: (Int, Int)) : Unit = {
    sumOfTile  = getSummForTile(tile)
    xMean  = getXMean(tile)
    powerOfTile  =  getPowerOfTwoForElementsAsSum(tile)
    standardDeviation = getStandartDeviationForTile(tile)
  }

  def getGstartForChildToo(paraParent : Settings, paraChild : Settings, childTile : Tile): (Tile, Tile) ={
    createNewWeight(paraParent)
    val parent = gStarComplete()
    calculateStats(0,0)
    createNewWeight(paraChild)
    val child = gStarComplete()
    (parent, child)
  }

  def getGstartForChildToo(paraParent : Settings, paraChild : Settings): (Tile, Tile) ={
    //createNewWeight(paraParent)

    val parent = gStarComplete()
    val size = (weight.cols,weight.rows)
    createNewWeight(paraChild)
    if(size._1<weight.cols || size._2<weight.rows){
      throw new IllegalArgumentException("Parent Weight must be greater than Child Weight")
    }
    val child = gStarComplete()
    (parent, child)
  }


  def gStarComplete(): Tile ={
    sumOfTile = this.getSummForTile(tile)
    xMean = this.getXMean(tile)
    standardDeviation= this.getStandartDeviationForTile(tile)
    val tileG = DoubleArrayTile.ofDim(tile.cols, tile.rows)
    for(i <- 0 to tile.cols-1){
      for(j <- 0 to tile.rows-1){
        tileG.setDouble(i,j,gStarForTile((i,j)))
      }
    }
    tileG
  }

  def gStarDoubleComplete(): Tile ={
    sumOfTile = this.getSummForTile(tile)
    xMean = this.getXMean(tile)
    standardDeviation= this.getStandartDeviationForTile(tile)
    val tileG = DoubleArrayTile.ofDim(tile.cols, tile.rows)
    for(i <- 0 to tile.cols-1){
      for(j <- 0 to tile.rows-1){
        tileG.setDouble(i,j,gStarForTileDouble((i,j)))
      }
    }
    tileG
  }

  override def createNewWeight(para : Settings) : Tile = {
    para.weightMatrix match {
      case Weight.One => weight = getWeightMatrix(para.weightRadius,para.weightRadius)
      case Weight.Square => weight = getWeightMatrixSquare(para.weightRadius)
      case Weight.Defined => weight = getWeightMatrixDefined(para.weightRadius,para.weightRadius)
      case Weight.Big => weight = getWeightMatrix(para.weightRadius,para.weightRadius)
      case Weight.High => weight = getWeightMatrixHigh()
      case Weight.Sigmoid => weight = getWeightMatrixSquareSigmoid(para.weightRadius,para.weightRadius/2)
    }



    sumOfWeight = this.getSummForTile(weight)
    powerOfWeight =  getPowerOfTwoForElementsAsSum(weight)
    weight
  }


  def getNumerator(index: (Int, Int)): Double={
    val xShift = Math.floor(weight.cols/2).toInt
    val yShift = Math.floor(weight.rows/2).toInt
    var sumP1 = 0
    for(i <- 0 to weight.cols-1){
      for(j <- 0 to weight.rows-1){
        if(index._1-xShift+i<0 || index._1-xShift+i>tile.cols-1 || index._2-yShift+j<0 || index._2-yShift+j>tile.rows-1){
          //TODO handle bound Cases
        } else {
          sumP1 += tile.get(index._1-xShift+i, index._2-yShift+j)*weight.get(i,j)
        }

      }
    }
    (sumP1-xMean*sumOfWeight)
  }

  def getNumeratorDouble(index: (Int, Int)): Double={
    val xShift = Math.floor(weight.cols/2).toInt
    val yShift = Math.floor(weight.rows/2).toInt
    var sumP1 = 0.0
    for(i <- 0 to weight.cols-1){
      for(j <- 0 to weight.rows-1){
        if(index._1-xShift+i<0 || index._1-xShift+i>tile.cols-1 || index._2-yShift+j<0 || index._2-yShift+j>tile.rows-1){
          //TODO handle bound Cases
        } else {
          sumP1 += tile.getDouble(index._1-xShift+i, index._2-yShift+j)*weight.get(i,j)
        }

      }
    }
    (sumP1-xMean*sumOfWeight)
  }

  def getDenominator(): Double = {
    (standardDeviation*Math.sqrt((tile.size*powerOfWeight-getSummPowerForWeight())/(tile.size-1)))
  }

  def gStarForTile(index : (Int, Int)) : Double ={
    getNumerator(index)/getDenominator()
  }

  def gStarForTileDouble(index : (Int, Int)) : Double ={
    getNumeratorDouble(index)/getDenominator()
  }

  def getStandartDeviationForTile(tile: Tile): Double ={
    val deviation = Math.sqrt((tile.mapDouble(x=>(Math.pow(x-xMean,2))).toArrayDouble().reduce(_+_))/(tile.size.toDouble-1))
    if(deviation<=0 || deviation==Double.NaN){
      return 1 //TODO handle equal distribution case
    }
    deviation
  }

  def getXMeanSquare(tile: Tile): Double ={
    xMean*xMean
  }



  def getSummPowerForWeight(): Double ={
    sumOfWeight*sumOfWeight
  }

  def getXMean(tile: Tile) : Double ={
    sumOfTile/tile.size
  }

  def getWeightMatrix(): ArrayTile = {
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




  def setCenter(array: Array[Array[Double]], cols: Int, rows: Int): Unit = {
    val centerCols =(cols/2.0).toInt
    val centerRows =(rows/2.0).toInt

    for(i <- 1 to 27) {
      for(j <- 1 to 27) {
        array(centerCols+i-13)(centerRows+j-13) = 0.9
      }
    }
    array(centerCols)(centerRows) = 1
  }

  def getWeightMatrixDefined(cols : Int, rows : Int): ArrayTile = {
    val arrayTile  = Array.ofDim[Double](cols*2+1,cols*2+1)

    for (i <- -cols to cols) {
      for (j <- -cols to cols) {
        if(Math.sqrt(i*i+j*j)<=cols) {
          arrayTile(cols + i)(cols + j) = 1.0
        } else {
          arrayTile(cols + i)(cols + j) = 0.1
        }
      }
    }
    val weightTile = new DoubleRawArrayTile(arrayTile.flatten, cols*2+1,cols*2+1)
    weightTile
  }

  def getWeightMatrix(cols : Int, rows : Int): ArrayTile ={
    val testTile = Array.fill(rows*cols)(1)
    val rasterTile = new IntRawArrayTile(testTile, cols, rows)
    rasterTile
  }


  def getWeightMatrixHigh(): ArrayTile ={
    val arrayTile = Array[Double](
      5, 5, 25, 5, 5,
      5, 25, 50.0, 25, 5,
      25, 50, 100, 50, 25,
      5, 25, 50, 25, 5,
      5, 5, 25, 5, 5)
    val weightTile = new DoubleRawArrayTile(arrayTile, 5,5)
    weightTile
  }


}




