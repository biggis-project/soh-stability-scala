package getisOrd


import datastructure.{MeasuersFocal, MeasuersGloabl}
import geotrellis.raster.mapalgebra.focal.{Circle, Kernel}
import geotrellis.{Spheroid, TimeNeighbourhood}
import geotrellis.raster.stitch.Stitcher.MultibandTileStitcher
import geotrellis.raster.{DoubleRawArrayTile, GridBounds, IntRawArrayTile, MultibandTile, Tile, TileLayout}
import geotrellis.spark.{Metadata, SpatialKey, TileLayerMetadata}
import geotrellis.vector.Extent
import importExport.{ImportGeoTiff, PathFormatter, ResultType, StringWriter}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import parmeters.Settings
import timeUtils.MultibandUtils

import scala.collection.mutable

/**
  * Created by marc on 03.07.17.
  */
object TimeGetisOrd {
  var timeMeasuresFocal = new MeasuersFocal()
  var timeMeasuresGlobal = new MeasuersGloabl()

  /**
    * Core functionallity for parallel G* calculation in geotemporalspace
    * @param rdd Rdd with raw data, partitionated
    * @param setting Parametrisation for calculatiosta
    * @param origin Unpartionated data for Testing and using for global value calcuation like standarddeviation, Focal G* does not use global values
    * @return The values of the statistic for each Pixel
    */
  def getGetisOrd(rdd : RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]], setting : Settings, origin : MultibandTile): MultibandTile ={
    timeMeasuresFocal = new MeasuersFocal
    timeMeasuresGlobal = new MeasuersGloabl

    val start = System.currentTimeMillis()

    var st : StatsGlobal= null
    if(!setting.focal){
      val startStats = System.currentTimeMillis()
      st = getSTGlobal(origin)
      timeMeasuresGlobal.setStats(System.currentTimeMillis()-startStats)
    }
    var counter = 0
    println("calcualted stats")
    val startBroadcast = System.currentTimeMillis()
    val broadcast = SparkContext.getOrCreate(setting.conf).broadcast(rdd.collect())
    timeMeasuresFocal.setBroadCast(System.currentTimeMillis()-startBroadcast)
    timeMeasuresGlobal.setBroadCast(System.currentTimeMillis()-startBroadcast)
    println("broadcast ended")

    val tiles = rdd.map(x=>{
      var result : MultibandTile = null
      counter += 1
      println("Run for key:"+x._1.toString+" counter:"+counter)
      if(setting.focal){
        result = getMultibandFocalGetisOrd(x._2, setting,x._1,getNeigbours(x._1,broadcast))

      } else {
        println("started G*")
        result = getMultibandGetisOrd(x._2, setting, st,x._1,getNeigbours(x._1,broadcast))
      }

      println("Finsished:"+x._1.toString+" counter:"+counter)
      (x._1,result)
    })
    val n = tiles.count()
    print("Counter:"+n)

    tiles.collect().map(x=>println("c,r"+x._2.cols+","+x._2.rows))

    var raster = tiles.stitch()

    println("raster,c,r"+raster.cols+","+raster.rows)
    println("origion,c,r"+origin.cols+","+origin.rows)
    val path = PathFormatter.getDirectory(setting, "partitions")

    assert(raster.dimensions==origin.dimensions)
    //(new ImportGeoTiff().writeMulitGeoTiff(tiles,setting,path+"all.tif"))
    val startWriting = System.currentTimeMillis()

    (new ImportGeoTiff().writeMultiTimeGeoTiffToSingle(raster,setting,path+"all.tif"))
    println("-----------------------------------------------------------------Start------------" +
      "---------------------------------------------------------------------------------------------------------")
    if(setting.focal){
      timeMeasuresFocal.setWriting(System.currentTimeMillis()-startWriting)
      timeMeasuresFocal.setAll(System.currentTimeMillis()-start)
      StringWriter.writeFile(timeMeasuresFocal.getPerformanceMetrik(),ResultType.Time,setting)
      println(timeMeasuresFocal.getPerformanceMetrik())
    } else {
      timeMeasuresGlobal.setWriting(System.currentTimeMillis()-startWriting)
      timeMeasuresGlobal.setAllTime(System.currentTimeMillis()-start)
      StringWriter.writeFile(timeMeasuresGlobal.getPerformanceMetrik(),ResultType.Time,setting)
      println(timeMeasuresGlobal.getPerformanceMetrik())
    }
    println("------------------------------------------------------------------End----------" +
      "----------------------------------------------------------------------------------------------------------")
    broadcast.destroy()
    raster
  }

  /**
    * Function for Focal G* calculation
    * @param mbT
    * @param setting
    * @return
    */
  def getMultibandFocalGetisOrd(mbT: MultibandTile, setting: Settings, position : SpatialKey, neigbours: mutable.HashMap[SpatialKey, MultibandTile]): MultibandTile = {
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
          val RMWNW2 = getRMWNW2(b, c, r, mbT, spheroidWeight, position, neigbours, getNM(b, c, r, mbT, spheroidFocal, position, neigbours))
          val sd = getSD(b, c, r, mbT, spheroidFocal, position, neigbours, RMWNW2.M)
          FocalGStar.band(b).asInstanceOf[DoubleRawArrayTile].setDouble(c,r,RMWNW2.getGStar(sd))
          timeMeasuresFocal.addBand(System.currentTimeMillis()-startBand)
        }

      }
      if(position._1==0 && position._2==0)
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
  def getMultibandGetisOrd(multibandTile: MultibandTile, setting: Settings, stats : StatsGlobal, position : SpatialKey, neighbours: mutable.HashMap[SpatialKey, MultibandTile]): MultibandTile = {
    val weightNeighbour = MultibandUtils.getWeight(setting,setting.weightRadius, setting.weightRadiusTime)

    var start = System.currentTimeMillis()
    val RoW = getSum(multibandTile, weightNeighbour, position,neighbours)
    if(position._1==0 && position._2==0)
      timeMeasuresGlobal.setRoW(System.currentTimeMillis()-start)


    println("End RoW")

    assert(multibandTile.cols==setting.layoutTileSize._1)
    assert(multibandTile.rows==setting.layoutTileSize._2)
    val extent = new Extent(0,0,setting.layoutTileSize._1, setting.layoutTileSize._2)

    start = System.currentTimeMillis()
    val MW = stats.gM*weightNeighbour.getSum() //W entry is allways 1
    if(position._1==0 && position._2==0)
      timeMeasuresGlobal.setMW(System.currentTimeMillis()-start)

    start = System.currentTimeMillis()
    val NW2 = stats.gN.toLong*weightNeighbour.getSum().toLong //W entry is allways 1
    if(position._1==0 && position._2==0)
      timeMeasuresGlobal.setNW2(System.currentTimeMillis()-start)

    start = System.currentTimeMillis()
    val W2 = Math.pow(weightNeighbour.getSum(),2)
    if(position._1==0 && position._2==0)
      timeMeasuresGlobal.setW2(System.currentTimeMillis()-start)

    start = System.currentTimeMillis()
    val denominator = stats.gS*Math.sqrt((NW2-W2)/(stats.gN-1))
    if(position._1==0 && position._2==0)
      timeMeasuresGlobal.setDenominator(System.currentTimeMillis()-start)

    //TODO
    assert(denominator>0)
    if(denominator>0){
      println(denominator+","+position.toString+","+stats.toString)

    }
    println("End Denminator")

    start = System.currentTimeMillis()
    val res = RoW.mapBands((band:Int,tile:Tile)=>tile.mapDouble(x=>{
      var result =((x-MW)/denominator)
      if(!isNotNaN(result)){
        result = 0
      }
      result
    }))
    if(position._1==0 && position._2==0)
      timeMeasuresGlobal.setDevision(System.currentTimeMillis()-start)

    res
  }


  private def getSD(b: Int, c: Int, r: Int, mbT: MultibandTile, spheroid : TimeNeighbourhood, position : SpatialKey, neigbours: mutable.HashMap[SpatialKey, MultibandTile], mean : Double): Double = {
    val radius = spheroid.a
    val radiusTime = spheroid.c
    var sd : Double = 0.0
    var n = 0
    for (x <- -radius to radius) {
      for (y <- -radius to radius) {
        for(z <- -radiusTime to radiusTime){
          if (spheroid.isInRange(x,y,z)) {
            var cx = c + x
            var ry = r + y
            var bz = (b + z) % 24
            if (bz < 0) {
              bz += 24
            }
            if (MultibandUtils.isInTile(c + x, r + y, mbT)) {
              sd += Math.pow(mbT.band(bz).getDouble(cx, ry)-mean,2)
              n += 1
            } else {
              val xShift = if (mbT.cols - 1 < cx) 1 else if (cx < 0) -1 else 0
              val yShift = if (mbT.rows - 1 < ry) 1 else if (ry < 0) -1 else 0
              if (neigbours.contains((position._1 + xShift, position._2 + yShift))) {
                cx = cx % (mbT.cols - 1)
                if (cx < 0) {
                  cx += mbT.cols
                }
                ry = ry % (mbT.rows - 1)
                if (ry < 0) {
                  ry += mbT.rows
                }
                sd += Math.pow(neigbours.get((position._1 + xShift, position._2 + yShift)).get.band(bz).getDouble(cx, ry)-mean,2)
                n += 1
              }
            }
          }
        }
      }
    }
    Math.sqrt(sd*(1.0 / (n.toDouble - 1.0)))
  }

  private def getRMWNW2(b: Int, c: Int, r: Int, mbT: MultibandTile, spheroid : TimeNeighbourhood, position : SpatialKey, neigbours: mutable.HashMap[SpatialKey, MultibandTile], nm : (Int,Double)): StatsRNMW = {
    val radius = spheroid.a
    val radiusTime = spheroid.c
    var row : Double = 0.0
    var wSum : Int= 0
    var start = System.currentTimeMillis()
    var neighbour = false
    for (x <- -radius to radius) {
      for (y <- -radius to radius) {
        for(z <- -radiusTime to radiusTime){
          if (spheroid.isInRange(x,y,z)) {
            var cx = c + x
            var ry = r + y
            var bz = (b + z) % 24
            if (bz < 0) {
              bz += 24
            }
            if (MultibandUtils.isInTile(c + x, r + y, mbT)) {
              row += mbT.band(bz).getDouble(cx, ry)
              wSum += 1
            } else {
              val xShift = if (mbT.cols - 1 < cx) 1 else if (cx < 0) -1 else 0
              val yShift = if (mbT.rows - 1 < ry) 1 else if (ry < 0) -1 else 0
              if (neigbours.contains((position._1 + xShift, position._2 + yShift))) {
                neighbour = true
                cx = cx % (mbT.cols - 1)
                if (cx < 0) {
                  cx += mbT.cols
                }
                ry = ry % (mbT.rows - 1)
                if (ry < 0) {
                  ry += mbT.rows
                }
                row += neigbours.get((position._1 + xShift, position._2 + yShift)).get.band(bz).getDouble(cx, ry)
                wSum += 1
              }
            }
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

  private def getNM(b: Int, c: Int, r: Int, mbT: MultibandTile, weightNeighbour : TimeNeighbourhood, position : SpatialKey, neigbours: mutable.HashMap[SpatialKey, MultibandTile]): (Int,Double) = {
    val radius = weightNeighbour.a
    val radiusTime = weightNeighbour.c
    var sum : Double = 0.0
    var count : Int= 0
    for (x <- -radius to radius) {
      for (y <- -radius to radius) {
        for(z <- -radiusTime to radiusTime){
          if (weightNeighbour.isInRange(x,y,z)) {
            var cx = c + x
            var ry = r + y
            var bz = (b + z) % 24
            if (bz < 0) {
              bz += 24
            }
            if (MultibandUtils.isInTile(c + x, r + y, mbT)) {
              sum += mbT.band(bz).getDouble(cx, ry)
              count += 1
            } else {
              val xShift = if (mbT.cols - 1 < cx) 1 else if (cx < 0) -1 else 0
              val yShift = if (mbT.rows - 1 < ry) 1 else if (ry < 0) -1 else 0
              if (neigbours.contains((position._1 + xShift, position._2 + yShift))) {
                cx = cx % (mbT.cols - 1)
                if (cx < 0) {
                  cx += mbT.cols
                }
                ry = ry % (mbT.rows - 1)
                if (ry < 0) {
                  ry += mbT.rows
                }
                sum += neigbours.get((position._1 + xShift, position._2 + yShift)).get.band(bz).getDouble(cx, ry)
                count += 1
              }
            }
          }
        }
      }
    }
    (count,sum/count.toDouble)
  }

  private def getSum(b: Int, c: Int, r: Int, mbT: MultibandTile, weightNeighbour : TimeNeighbourhood, position : SpatialKey, neigbours: mutable.HashMap[SpatialKey, MultibandTile]): Double = {
    val radius = weightNeighbour.a
    val radiusTime = weightNeighbour.c
    var sum : Double = 0.0
    for (x <- -radius to radius) {
      for (y <- -radius to radius) {
        for(z <- -radiusTime to radiusTime){
          if (weightNeighbour.isInRange(x,y,z)) {
            var cx = c + x
            var ry = r + y
            var bz = (b + z) % 24
            if (bz < 0) {
              bz += 24
            }
            if (MultibandUtils.isInTile(c + x, r + y, mbT)) {
              sum += mbT.band(bz).getDouble(cx, ry)
            } else {


              val xShift = if (mbT.cols - 1 < cx) 1 else if (cx < 0) -1 else 0
              val yShift = if (mbT.rows - 1 < ry) 1 else if (ry < 0) -1 else 0
              if (neigbours.contains((position._1 + xShift, position._2 + yShift))) {
                cx = cx % (mbT.cols - 1)
                if (cx < 0) {
                  cx += mbT.cols
                }
                ry = ry % (mbT.rows - 1)
                if (ry < 0) {
                  ry += mbT.rows
                }
                sum += neigbours.get((position._1 + xShift, position._2 + yShift)).get.band(bz).getDouble(cx, ry)
              }
            }
          }
        }
      }
    }
    sum
  }

  private def getSum(mbT: MultibandTile, weightNeighbour : TimeNeighbourhood, position : SpatialKey, neigbours: mutable.HashMap[SpatialKey, MultibandTile]): MultibandTile = {
    val multibandTile: MultibandTile = MultibandUtils.getEmptyMultibandArray(mbT)

    for(b <- 0 to mbT.bandCount-1){
      val singleBand = multibandTile.band(b).asInstanceOf[DoubleRawArrayTile]
      for(c <- 0 to multibandTile.cols-1){
        for(r <- 0 to multibandTile.rows-1){
          singleBand.setDouble(c,r, getSum(b,c,r,mbT, weightNeighbour, position, neigbours))
        }
      }
    }
    multibandTile


  }




  def isNotNaN(f: Double): Boolean = {
    val r = (f== -2147483648 ||f.isNaN || f.isNegInfinity || f.isInfinity)
    !r
  }




  private def getSTGlobal(origin : MultibandTile): StatsGlobal = {
    val gN = (origin.bandCount * origin.rows * origin.cols)

    val gM = origin.bands.map(x => x.toArrayDouble().filter(x => isNotNaN(x)).reduce(_ + _)).reduce(_ + _)/gN

    val singelSDBand = origin.bands.map(x => x.toArrayDouble().filter(x => isNotNaN(x)).map(x => Math.pow(x - gM, 2)).reduce(_ + _))
    val gS = Math.sqrt((singelSDBand.reduce(_ + _)) * (1.0 / (gN.toDouble - 1.0)))
    new StatsGlobal(gN,gM,gS)
  }

  private def isInRange(newKey: SpatialKey, max : SpatialKey): Boolean = {
    val r = newKey._2>=0 && newKey._1>=0 && newKey._1<=max._1 && newKey._2<=max._2
    r
  }

  private def getNeigbours(key : SpatialKey, broadcast: Broadcast[Array[(SpatialKey, MultibandTile)]]): mutable.HashMap[SpatialKey,MultibandTile] ={
    println("starated Neighbours*")
    val max = broadcast.value.map(x=>x._1).max
    var hashMap  = new mutable.HashMap[SpatialKey,MultibandTile]()
    for(i <- -1 to 1) {
      for (j <- -1 to 1) {
        val newKey = new SpatialKey(key._1 + i, key._2 + j)
        if(isInRange(newKey, max) && Math.abs(i)+Math.abs(j)!=0){
          val lookUp = broadcast.value.filter(y=>y._1==newKey)
          lookUp.map(x=>hashMap.put(x._1,x._2))
        }

      }
    }
    println("End Neighbours*")
    return hashMap
  }






}

class StatsRNMW(val RoW : Double, val N : Double, val M : Double, val MW : Double, val NW2 : Double, val W2 : Double) extends Serializable{
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

class StatsFocal(val fN : MultibandTile, val fM : MultibandTile, val fS :MultibandTile, val MW: MultibandTile, val NW2: MultibandTile, val W2 : MultibandTile) extends Serializable{

}

class StatsGlobal(val gN : Int, val gM : Double, val gS :Double) extends Serializable{
  override def toString: String = "stats(N,M,S):("+gN+","+gM+","+gS+")"
}