package importExport

import java.io.File

import geotrellis.proj4.CRS
import geotrellis.raster.{MultibandTile, Tile, TileLayout}
import geotrellis.raster.io.geotiff.{MultibandGeoTiff, SinglebandGeoTiff}
import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import geotrellis.raster.resample.NearestNeighbor
import geotrellis.spark.{SpatialKey, TileLayerMetadata}
import geotrellis.vector.{Extent, ProjectedExtent}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import parmeters.Settings
import geotrellis.spark.io.hadoop.{HadoopAttributeStore, HadoopLayerReader, HadoopLayerWriter, HadoopSparkContextMethodsWrapper}
import geotrellis.spark.tiling.FloatingLayoutScheme
import geotrellis.spark.{LayerId, SpatialKey, TileLayerMetadata, withProjectedExtentTilerKeyMethods, withTileRDDReprojectMethods, withTilerMethods, _}
/**
  * Created by marc on 02.06.17.
  * Used to read and write Tiffs
  */
class ImportGeoTiff {
  def writeGeoTiff(tile: Tile, file: String, settings: Settings): Unit = {
    SinglebandGeoTiff.apply(tile, new Extent(settings.buttom._1,settings.buttom._2,settings.top._1,settings.top._2), crs).write(file)
  }

  val crs = CRS.fromName("EPSG:3857")

  def geoTiffExists(globalSettings: Settings, extra : String): Boolean = {
    geoTiffExists(getFileName(globalSettings, extra))

  }

  def getFileName(globalSettings: Settings, extra : String): String = {
    (PathFormatter.getDirectory(globalSettings, extra) + "aggregation_" + globalSettings.aggregationLevel +"w_"+globalSettings.weightRadius+"h_"+globalSettings.hour+".tif")
  }

  def geoTiffExists(file : String): Boolean ={
    new File(file).exists()
  }

  def getGeoTiff(setting : Settings, extra : String): Tile ={
    getGeoTiff(getFileName(setting,extra))
  }

  def getMulitGeoTiff(setting : Settings, extra : String): MultibandTile ={
    getMulitGeoTiff(getFileName(setting,extra))
  }

  def getMulitGeoTiff(setting : Settings, tifType: TifType.Value): MultibandTile ={
    getMulitGeoTiff(PathFormatter.getDirectoryAndName(setting,tifType))
  }

  def getMulitGeoTiff(file : String): MultibandTile = {
    println(file)
    GeoTiffReader.readMultiband(file)
  }

  def repartitionFiles(setting: Settings): RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]] ={
    val file = PathFormatter.getDirectoryAndName(setting, TifType.Raw)
    repartitionFiles(file,setting)
  }

  def repartitionFiles(file : String,setting: Settings): RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]] ={
    val sc = SparkContext.getOrCreate(setting.conf)
    val origion = getMulitGeoTiff(file)
    val inputRdd: RDD[(ProjectedExtent, MultibandTile)] =
      sc.hadoopMultibandGeoTiffRDD(file)

    val layoutScheme = FloatingLayoutScheme(setting.layoutTileSize._1,setting.layoutTileSize._2)
    val (_, rasterMetaData) =
      TileLayerMetadata.fromRdd(inputRdd, crs, layoutScheme)
    val tiled: RDD[(SpatialKey, MultibandTile)] =
      inputRdd
        .tileToLayout(rasterMetaData.cellType, rasterMetaData.layout, NearestNeighbor)
        .repartition(4)
    tiled.foreach(f=>{
      println(f._2.cols+","+origion.cols/2 +","+ f._2.rows+","+origion.rows/2)
      assert(f._2.cols==origion.cols/2 && f._2.rows==origion.rows/2)
    })

    val rddWithContext: RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]] = ContextRDD(tiled, rasterMetaData)
    rddWithContext
  }

  def readGeoTiff(file : String): Tile = {
    val tiff = GeoTiffReader.readSingleband(file)//, new Extent(3574113.2199997901916504,5407261.9099998474121094,3581419.2199997901916504,5413584.9099998474121094))
    tiff.tile
  }

  def readGeoTiff(settings: Settings, tifType: TifType.Value): Tile = {
    GeoTiffReader.readSingleband(PathFormatter.getDirectoryAndName(settings,tifType,false)).tile
  }

  def getGeoTiff(file : String): Tile ={
    getMulitGeoTiff(file).band(0)
  }

  def writeGeoTiff(tile: Tile, settings: Settings, tifType: TifType.Value): Unit = {
    val name = PathFormatter.getDirectoryAndName(settings,tifType,false)
    writeGeoTiff(tile: Tile,name, settings: Settings)
  }

  def writeGeoTiff(tile: Tile, settings: Settings, extra : String): Unit = {
    val name = getFileName(settings, extra)

    writeGeoTiff(tile: Tile,name, settings: Settings)
  }

  def writeMultiGeoTiff(tile: MultibandTile, settings: Settings, tifType: TifType.Value): Unit = {
    writeMultiGeoTiff(tile,settings, PathFormatter.getDirectoryAndName(settings,tifType))
  }


  def writeMultiGeoTiff(tile: MultibandTile, para: Settings, file : String): Unit = {
    val extent = new Extent(para.buttom._1,para.buttom._2,para.top._1,para.top._2)
    writeMultiGeoTiff(tile,extent, file)
  }

  def writeMultiGeoTiff(tile: MultibandTile, extent :Extent, file : String): Unit = {
    MultibandGeoTiff.apply(tile, extent,crs).write(file)
  }

  def writeMultiTimeGeoTiffToSingle(tile: MultibandTile, para: Settings, file : String): Unit = {
    for(i <- 0 to tile.bandCount-1){
      SinglebandGeoTiff.apply(tile.band(i), new Extent(para.buttom._1,para.buttom._2,para.top._1,para.top._2), crs).write(file+"hour_"+i.formatted("%02d")+".tif")
    }
  }

  def writeMultiTimeGeoTiffToSingle(tile: MultibandTile, para: Settings, file : String, origin : Tile): Unit = {
    val layout : TileLayout= new TileLayout(origin.cols,origin.rows,origin.cols,origin.rows)
    for(i <- 0 to tile.bandCount-1){
      val bandTile = tile.band(i) //.split(layout)(0)
      assert(bandTile.dimensions==origin.dimensions)
      SinglebandGeoTiff.apply(bandTile, new Extent(para.buttom._1,para.buttom._2,para.top._1,para.top._2), crs).write(file+"hour_"+i.formatted("%02d")+".tif")
    }
  }
}
