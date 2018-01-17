package getisOrd


import datastructure.{MeasuersFocal, MeasuersGloabl}
import geotrellis.TimeNeighbourhood
import geotrellis.raster.mapalgebra.focal.Kernel
import geotrellis.raster.stitch.Stitcher.MultibandTileStitcher
import geotrellis.raster.{DoubleRawArrayTile, GridBounds, MultibandTile, Raster, Tile}
import geotrellis.spark.{ContextRDD, Metadata, SpatialKey, TileLayerMetadata}
import importExport.{ImportGeoTiff, PathFormatter, ResultType, StringWriter}
import org.apache.spark.rdd.RDD
import parmeters.Settings
import timeUtils.MultibandUtils
import geotrellis.spark.stitch._
/**
  * Created by marc on 03.07.17.
  */
object TimeGetisOrd {

  /**
    * Core functionallity for parallel G* calculation in geotemporalspace
    * @param rdd Rdd with raw data, partitionated
    * @param setting Parametrisation for calculatiosta
    * @param origin Unpartionated data for Testing and using for global value calcuation like standarddeviation, Focal G* does not use global values
    * @return The values of the statistic for each Pixel
    */
  def getGetisOrd(rdd : RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]], setting : Settings, origin : MultibandTile): MultibandTile = {
    timeMeasuresFocal = new MeasuersFocal
    timeMeasuresGlobal = new MeasuersGloabl
    val start = System.currentTimeMillis()
    val focalKernel = Kernel.circle(2*setting.focalRange+1, setting.focalRange,setting.focalRange)

    var st : StatsGlobal= null
    var r : ContextRDD[SpatialKey, MultibandTile, TileLayerMetadata[SpatialKey]] = null
    if(!setting.focal){
      val startStats = System.currentTimeMillis()
      st = getSTGlobal(origin, setting)
      timeMeasuresGlobal.setStats(System.currentTimeMillis()-startStats)
      r = rdd.withContext { x => x.
        bufferTiles(focalKernel.extent)
        //.mapValues{ mbT => mbT.tile}
        .mapValues{ mbT => crop(getMultibandGetisOrd(mbT.tile,setting,st), mbT.targetArea)}
      }
    } else {
      r = rdd.withContext { x => x.
        bufferTiles(focalKernel.extent)
        //.mapValues{ mbT => mbT.tile}
        .mapValues{ mbT => crop(getMultibandFocalGetisOrd(mbT.tile, setting), mbT.targetArea)}
      }
    }
    val raster = r.stitch()
    raster
  }

  def crop(mbT: MultibandTile, gridBounds: GridBounds) = {
    val bands = mbT.bandCount
    val size = gridBounds.height
    println("Size:"+size)
    println("ColMin,RowMin"+gridBounds.colMin+","+gridBounds.rowMin)
    println("ColMax,RowMax"+mbT.cols+","+mbT.rows)
    var bandArray = new Array[DoubleRawArrayTile](bands)
    for (b <- 0 to bands-1) {
      bandArray(b) = new DoubleRawArrayTile(Array.fill(size*size)(0), size, size)
      for(r <- 0 to size-1){
        for(c <- 0 to size-1){
          bandArray(b).setDouble(c,r, mbT.band(b).getDouble(c+gridBounds.colMin,r+gridBounds.rowMin))
        }
      }
    }

    val multibandTile = MultibandTile.apply(bandArray)
    multibandTile
  }

  var timeMeasuresFocal = new MeasuersFocal()
  var timeMeasuresGlobal = new MeasuersGloabl()

  /**
    * Function for Focal G* calculation
    * @param mbT
    * @param setting
    * @return
    */
  private def getMultibandFocalGetisOrd(mbT: MultibandTile, setting: Settings): MultibandTile = {
    val spheroidFocal = MultibandUtils.getWeight(setting,setting.focalRange, setting.focalRangeTime)
    val spheroidWeight = MultibandUtils.getWeight(setting,setting.weightRadius, setting.weightRadiusTime)
    val radius = spheroidFocal.a
    val radiusTime = spheroidFocal.c
    val FocalGStar: MultibandTile = MultibandUtils.getEmptyMultibandArray(mbT)

    for (r <- 0 to mbT.rows - 1) {
      println("Next r:"+r)
      var start = System.currentTimeMillis()
      for (c <- 0 to mbT.cols - 1) {
        for (b <- 0 to mbT.bandCount - 1) {
          var startBand = System.currentTimeMillis()
          val RMWNW2 = getRMWNW2(b, c, r, mbT, spheroidWeight, getNM(b, c, r, mbT, spheroidFocal))
          val sd = getSD(b, c, r, mbT, spheroidFocal, RMWNW2.M)
          FocalGStar.band(b).asInstanceOf[DoubleRawArrayTile].setDouble(c,r,RMWNW2.getGStar(sd))
          timeMeasuresFocal.addBand(System.currentTimeMillis()-startBand)
        }

      }
        timeMeasuresFocal.addRow(System.currentTimeMillis()-start)

    }
    FocalGStar
  }

  /**
    * Function for G* calculation
    * @param multibandTile
    * @param setting
    * @param stats
    * @return
    */
  def getMultibandGetisOrd(multibandTile: MultibandTile, setting: Settings, stats : StatsGlobal): MultibandTile = {
    val weightNeighbour = MultibandUtils.getWeight(setting,setting.weightRadius, setting.weightRadiusTime)

    var start = System.currentTimeMillis()
    val RoW = getSum(multibandTile, weightNeighbour)
    start = System.currentTimeMillis()

    val res = RoW.mapBands((band:Int,tile:Tile)=>tile.mapDouble(x=>{
      var result =((x-stats.MW)/stats.denominator)
      if(!isNotNaN(result)){
        result = 0
      }
      result
    }))
    timeMeasuresGlobal.setDevision(System.currentTimeMillis()-start)
    res
  }

  private def getSD(b: Int, c: Int, r: Int, mbT: MultibandTile, spheroid : TimeNeighbourhood, mean : Double): Double = {
    val radius = spheroid.a
    val radiusTime = spheroid.c
    var sd : Double = 0.0
    var n = 0
    for (x <- -radius to radius) {
      for (y <- -radius to radius) {
        for(z <- -radiusTime to radiusTime){
          if (spheroid.isInRange(x,y,z)) {
            var bz = (b + z) % 24
            if (bz < 0) {
              bz += 24
            }
            sd += Math.pow(mbT.band(bz).getDouble(c, r)-mean,2)
            n += 1

          }
        }
      }
    }
    Math.sqrt(sd*(1.0 / (n.toDouble - 1.0)))
  }

  private def getRMWNW2(b: Int, c: Int, r: Int, mbT: MultibandTile, spheroid : TimeNeighbourhood, nm : (Int,Double)): StatsRNMW = {
    val radius = spheroid.a
    val radiusTime = spheroid.c
    var row: Double = 0.0
    var wSum: Int = 0
    var start = System.currentTimeMillis()
    var neighbour = false
    for (x <- -radius to radius) {
      for (y <- -radius to radius) {
        for (z <- -radiusTime to radiusTime) {
          if (spheroid.isInRange(x, y, z)) {
            var bz = (b + z) % 24
            if (bz < 0) {
              bz += 24
            }
            row += mbT.band(bz).getDouble(c, r)
            wSum += 1

          }
        }
      }
    }

    val mw = wSum*nm._2
    val nw2 = wSum*nm._1
    val w2 =Math.pow(wSum,2)

    if(neighbour){
     timeMeasuresFocal.addNeighbour(System.currentTimeMillis()-start)
    } else {
     timeMeasuresFocal.addNoNeighbour(System.currentTimeMillis()-start)
    }

    new StatsRNMW(row,nm._1,nm._2,mw,nw2,w2)
  }

  private def getNM(b: Int, c: Int, r: Int, mbT: MultibandTile, weightNeighbour : TimeNeighbourhood): (Int,Double) = {
    val radius = weightNeighbour.a
    val radiusTime = weightNeighbour.c
    var sum : Double = 0.0
    var count : Int= 0
    for (x <- -radius to radius) {
      for (y <- -radius to radius) {
        for(z <- -radiusTime to radiusTime){
          if (weightNeighbour.isInRange(x,y,z)) {
            var bz = (b + z) % 24
            if (bz < 0) {
              bz += 24
            }
            sum += mbT.band(bz).getDouble(c, r)
            count += 1

          }
        }
      }
    }
    (count,sum/count.toDouble)
  }

  private def getSum(b: Int, c: Int, r: Int, mbT: MultibandTile, weightNeighbour : TimeNeighbourhood): Double = {
    val radius = weightNeighbour.a
    val radiusTime = weightNeighbour.c
    var sum : Double = 0.0
    for (x <- -radius to radius) {
      for (y <- -radius to radius) {
        for(z <- -radiusTime to radiusTime){
        if (weightNeighbour.isInRange(x,y,z)) {
          var bz = (b + z) % 24
          if (bz < 0) {
            bz += 24
          }
          sum += mbT.band(bz).getDouble(c, r)
        }
        }
      }
    }
    sum
  }

  private def getSum(mbT: MultibandTile, weightNeighbour : TimeNeighbourhood): MultibandTile = {
    val multibandTile: MultibandTile = MultibandUtils.getEmptyMultibandArray(mbT)

    for(b <- 0 to mbT.bandCount-1){
      val singleBand = multibandTile.band(b).asInstanceOf[DoubleRawArrayTile]
      for(c <- 0 to multibandTile.cols-1){
        for(r <- 0 to multibandTile.rows-1){
          singleBand.setDouble(c,r, getSum(b,c,r,mbT, weightNeighbour))
        }
      }
    }
    multibandTile
  }

  private def isNotNaN(f: Double): Boolean = {
    val r = (f== -2147483648 ||f.isNaN || f.isNegInfinity || f.isInfinity)
    !r
  }

  private def WriteTimeMeasurring(setting: Settings, origin: MultibandTile, start: Long, raster: Raster[MultibandTile]): Unit = {
    println("raster,c,r" + raster.cols + "," + raster.rows)
    println("origion,c,r" + origin.cols + "," + origin.rows)
    val path = PathFormatter.getDirectory(setting, "partitions")

    assert(raster.dimensions == origin.dimensions)
    //(new ImportGeoTiff().writeMulitGeoTiff(tiles,setting,path+"all.tif"))
    val startWriting = System.currentTimeMillis()

    (new ImportGeoTiff().writeMultiTimeGeoTiffToSingle(raster, setting, path + "all.tif"))
    println("-----------------------------------------------------------------Start------------" +
      "---------------------------------------------------------------------------------------------------------")
    if (setting.focal) {
      timeMeasuresFocal.setWriting(System.currentTimeMillis() - startWriting)
      timeMeasuresFocal.setAll(System.currentTimeMillis() - start)
      StringWriter.writeFile(timeMeasuresFocal.getPerformanceMetrik(), ResultType.Time, setting)
      println(timeMeasuresFocal.getPerformanceMetrik())
    } else {
      timeMeasuresGlobal.setWriting(System.currentTimeMillis() - startWriting)
      timeMeasuresGlobal.setAllTime(System.currentTimeMillis() - start)
      StringWriter.writeFile(timeMeasuresGlobal.getPerformanceMetrik(), ResultType.Time, setting)
      println(timeMeasuresGlobal.getPerformanceMetrik())
    }
    println("------------------------------------------------------------------End----------" +
      "----------------------------------------------------------------------------------------------------------")
  }

  private def getSTGlobal(origin : MultibandTile, setting : Settings): StatsGlobal = {
    val weightNeighbour = MultibandUtils.getWeight(setting,setting.weightRadius, setting.weightRadiusTime)
    val gN = (origin.bandCount * origin.rows * origin.cols)
    val gM = origin.bands.map(x => x.toArrayDouble().filter(x => isNotNaN(x)).reduce(_ + _)).reduce(_ + _)/gN
    val singelSDBand = origin.bands.map(x => x.toArrayDouble().filter(x => isNotNaN(x)).map(x => Math.pow(x - gM, 2)).reduce(_ + _))
    val gS = Math.sqrt((singelSDBand.reduce(_ + _)) * (1.0 / (gN.toDouble - 1.0)))
    val MW = gM*weightNeighbour.getSum() //W entry is allways 1
    val NW2 =gN.toLong*weightNeighbour.getSum().toLong //W entry is allways 1
    val W2 = Math.pow(weightNeighbour.getSum(),2)
    val denominator = gS*Math.sqrt((NW2-W2)/(gN-1))
    new StatsGlobal(gN, gM, gS, MW,denominator)
  }

  private def isInRange(newKey: SpatialKey, max : SpatialKey): Boolean = {
    val r = newKey._2>=0 && newKey._1>=0 && newKey._1<=max._1 && newKey._2<=max._2
    r
  }

  private class StatsRNMW(val RoW : Double, val N : Double, val M : Double, val MW : Double, val NW2 : Double, val W2 : Double) extends Serializable{
    def getNominator(): Double ={
      RoW-MW
    }

    def getDenominator(sd : Double): Double ={
      sd*Math.sqrt((NW2-W2)/(N-1))
    }

    def getGStar(sd : Double) : Double = {
      if(sd==0){
        return 0
      }
      val r = getNominator()/getDenominator(sd)
      if(TimeGetisOrd.isNotNaN(r)){
        return r
      } else {
        return 1
      }
    }

    override def equals(obj: scala.Any): Boolean = {
      if(obj.isInstanceOf[StatsRNMW]){
        val compa = obj.asInstanceOf[StatsRNMW]
        return compa.M==this.M &&
          compa.N==this.N &&
          compa.RoW==this.RoW &&
          compa.MW==this.MW &&
          compa.NW2==this.NW2 &&
          compa.W2 == this.W2
      } else {
        return false
      }
    }
  }

  private class StatsFocal(val fN : MultibandTile, val fM : MultibandTile, val fS :MultibandTile, val MW: MultibandTile, val NW2: MultibandTile, val W2 : MultibandTile) extends Serializable{

  }

  private class StatsGlobal(val gN : Int, val gM : Double, val gS :Double, val denominator : Double, val MW : Double) extends Serializable{
    override def toString: String = "stats(N,M,S):("+gN+","+gM+","+gS+")"
  }



}


