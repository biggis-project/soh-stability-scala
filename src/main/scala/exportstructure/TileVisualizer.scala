package exportstructure

import java.awt.Color
import java.awt.image.BufferedImage
import java.io.{File, FileOutputStream}
import java.time.LocalDateTime
import java.util.Random
import javax.imageio.ImageIO

import geotrellis.proj4.CRS
import geotrellis.raster
import geotrellis.raster.io.geotiff.SinglebandGeoTiff
import geotrellis.raster.resample.Bilinear
import geotrellis.raster.{DoubleRawArrayTile, Raster, Tile}
import geotrellis.spark.{SpatialKey, TileLayerMetadata}
import geotrellis.spark.tiling.FloatingLayoutScheme
import geotrellis.vector.{Extent, ProjectedExtent}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import parmeters.Settings
import geotrellis.spark.io.hadoop.{HadoopAttributeStore, HadoopLayerDeleter, HadoopLayerReader, HadoopLayerWriter, HadoopSparkContextMethodsWrapper}
import geotrellis.spark.{LayerId, TileLayerMetadata, TileLayerRDD, withProjectedExtentTilerKeyMethods, withTileRDDReprojectMethods, withTilerMethods}
import importExport.PathFormatter
/**
  * Created by marc on 08.05.17.
  */
class TileVisualizer {
  def visualCluster(tile: Tile, name: String): Unit = {
    val bfI = new BufferedImage(tile.cols, tile.rows, BufferedImage.TYPE_INT_RGB);
    var content: Int = 0
    for (i <- 0 to tile.cols - 1) {
      for (j <- 0 to tile.rows - 1) {
        content = (tile.get(i, j))
        bfI.setRGB(i, j, (new Color(Math.min(content,255), 0, 0)).getRGB)
      }
    }
    val fos = new FileOutputStream("/home/marc/Masterarbeit/outPut/" + name + ".png");
    ImageIO.write(bfI, "PNG", fos);
    fos.close();
  }


  def visualCluster(cluster : (Tile,Int), name: String): Unit = {
    val bfI = new BufferedImage(cluster._1.cols, cluster._1.rows, BufferedImage.TYPE_INT_RGB);
    var content: Int = 0
    for (i <- 0 to cluster._1.cols - 1) {
      for (j <- 0 to cluster._1.rows - 1) {
        content = (cluster._1.get(i, j))

        if(content>cluster._2-10){
          bfI.setRGB(i, j, (new Color(0,0,Math.min((content-(cluster._2-10))*25,255))).getRGB)
        } else {
          bfI.setRGB(i, j, (new Color(Math.min(content,255), 0, 0)).getRGB)
        }

      }
    }
    val fos = new FileOutputStream("/home/marc/Masterarbeit/outPut/" + name + ".png");
    ImageIO.write(bfI, "PNG", fos);
    fos.close();
  }

  def visualTileOld(tile: Tile, name: String, para : Settings): Unit = {
    val bfI = new BufferedImage(tile.cols, tile.rows, BufferedImage.TYPE_INT_RGB);
    val max = tile.toArrayDouble().max
    val min = tile.toArrayDouble().min

    val rangeFactorRed = 200 / (max)
    var rangeFactorBlue = 0.0
    if (min < 0) {
      rangeFactorBlue = 200 / (Math.abs(min))
    }

    val red = new Color(255, 0, 0)
    val blue = new Color(0, 0, 255)
    var content: Double = 0.0
    for (i <- 0 to tile.cols - 1) {
      for (j <- 0 to tile.rows - 1) {
        content = (tile.getDouble(i, j))
        if (content > 0) {
          bfI.setRGB(i, j, (new Color((content * rangeFactorRed).ceil.toInt + 30, 0, 0)).getRGB)
        } else {
          bfI.setRGB(i, j, (new Color(0, 0, (-1 * rangeFactorBlue * content).ceil.toInt + 30)).getRGB)
        }

      }
    }
    val fos = new FileOutputStream(para.ouptDirectory + name + ".png");
    ImageIO.write(bfI, "PNG", fos);
    fos.close();


  }



  def visualTileNew(tile: Tile, para : Settings, extra : String): Unit = {

    val bfI = new BufferedImage(tile.cols, tile.rows, BufferedImage.TYPE_INT_RGB);
    val minMax = tile.findMinMax
    println("min,max"+minMax)


    var content: Double = 0.0
    for (i <- 0 to tile.cols - 1) {
      for (j <- 0 to tile.rows - 1) {
        if(raster.isData(tile.getDouble(i,j))){
          content = (tile.getDouble(i, j))
          bfI.setRGB(i, j, (logScale(content,minMax._1,minMax._2)).getRGB)
        } else {
          bfI.setRGB(i, j, 0)
        }
      }
    }

    val fileName = PathFormatter.getDirectory(para, extra) +tile.rows+"______"+para.weightMatrix+"r_"+para.weightRadius+"_"+tile.cols+"Time_"+ LocalDateTime.now().formatted("HH_mm" ) + ".png"
    if(!new File(fileName).exists()){
      return
    }
    val fos = new FileOutputStream(fileName);
    ImageIO.write(bfI, "PNG", fos);
    fos.close();


  }



  def tableScale(min : Double, max : Double, n : Double): Color ={
    //Hue value
    //https://www.w3schools.com/colors/colors_picker.asp?colorhex=ff0000
    val hue 	= Array(new Color(255, 0, 0),
      new Color(255, 64, 0),
      new Color(255, 128, 0),
      new Color(255, 191, 0),
      new Color(255, 255, 0),
      new Color(191, 255, 0),
      new Color(128, 255, 0),
      new Color(64, 255, 0),
      new Color(0, 255, 0),
      new Color(0, 255, 64),
      new Color(0, 255, 128),
      new Color(0, 255, 191),
      new Color(0, 255, 255),
      new Color(0, 191, 255),
      new Color(0, 128, 255),
      new Color(0, 64, 255),
      new Color(0, 0, 255))
    val range = Math.abs(min)+max
    val index : Int = ((n+Math.abs(min))/(hue.length/range)).toInt
    return hue(index)

  }

  def logScale(n : Double, min : Double, max : Double): Color ={
//    val red = new Color(255, 0, 0)
//    val blue = new Color(0, 0, 255)
    if(n==0){
      return new Color(0,0,0)
    } else if(n<0){
      val blueValue = ((Math.log(-1*n+1)*(255/Math.log(Math.abs(min)+1))).toInt)
      if(blueValue>=255 && blueValue<=0){
        //println(blueValue)
      }
      return new Color(0,0,Math.min(255,blueValue))
    } else {
      val redValue = (Math.log(n+1)*(255/Math.log(max+1))).toInt
      if(redValue>255){
        //println(redValue)
      }
      return new Color(Math.min(255,redValue),0,0)
    }

  }

  def linearScale(n : Double, min : Double, max : Double): Color ={
    if(n==0){
      return new Color(0,0,0)
    } else if(n<0){
      val blueValue = (-1*n*(255/Math.abs(min))).toInt
      return new Color(0,0,Math.min(255,blueValue))
    } else {
      val redValue = (n * 255/max).ceil.toInt
      return new Color(Math.min(255,redValue),0,0)
    }
  }

  def visualTile(tile: Tile, para: Settings, extra : String): Unit = {
    val fos = new FileOutputStream(para.ouptDirectory + extra+"focal"+ para.focal +"_"+ "parent" + para.parent +"_" +
    para.weightMatrix+"r_"+para.weightRadius+"_cluster_meta_"+tile.rows+"_"+tile.cols+ LocalDateTime.now().formatted("MM_dd_HH_mm_ss" ) + ".png");
    ImageIO.write(tile.toBufferedImage, "PNG", fos);
    fos.close();
  }
}
