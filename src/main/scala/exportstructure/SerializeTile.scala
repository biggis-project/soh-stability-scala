package exportstructure

import java.io.{FileInputStream, FileOutputStream, ObjectInputStream, ObjectOutputStream}

import geotrellis.proj4.CRS
import geotrellis.raster.io.geotiff.SinglebandGeoTiff
import geotrellis.raster.resample.Bilinear
import geotrellis.raster.{DoubleArrayTile, FloatArrayTile, MultibandTile, Tile}
import geotrellis.spark.{SpatialKey, TileLayerMetadata}
import geotrellis.vector.{Extent, ProjectedExtent}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import parmeters.Settings
import geotrellis.spark.io.hadoop.HadoopSparkContextMethodsWrapper
import geotrellis.spark.tiling.FloatingLayoutScheme
import geotrellis.spark.withTilerMethods
/**
  * Created by marc on 11.05.17.
  */
class SerializeTile(path : String) {

  def write(tile : Tile): Unit ={
    val oos = new ObjectOutputStream(new FileOutputStream(path))
    oos.writeObject(tile)
    oos.close
  }

  def read(): Tile = {
    val ois = new ObjectInputStream(new FileInputStream(path))
    val tile = ois.readObject.asInstanceOf[Tile]
    ois.close
    tile
  }
}
