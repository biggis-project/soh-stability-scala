package scripts

import clustering.{ClusterHotSpotsTime, ClusterRelations}
import geotrellis.raster.IntRawArrayTile
import getisOrd.{SoH, TimeGetisOrd}
import importExport.{ImportGeoTiff, PathFormatter, TifType}
import parmeters.Settings
import rasterTransformation.Transformation
import timeUtils.MultibandUtils

/**
  * Created by marc on 05.09.17.
  */
object ZoomScript {
  def main(args: Array[String]): Unit = {
    val export = new ImportGeoTiff

    var validate = new ClusterRelations()
    val transform = new Transformation

    //weight , weightTime , focalWeight , focalTime , aggregationLevel , month , zoom
    var settings = MetrikValidation.getBasicSettings(5,1,10,2,4,1,1)
    settings.focal = true
    writeBand(settings, export)
    val f2 = export.getMulitGeoTiff(settings,TifType.Cluster)
    settings = MetrikValidation.getBasicSettings(5,1,10,2,4,1,3)
    settings.focal = true
    writeBand(settings, export)
    val f1 = export.getMulitGeoTiff(settings,TifType.Cluster)

    val flessRows = f2.rows-f1.rows
    val flessCols = f2.cols-f1.cols

    val fpart = MultibandUtils.getEmptyIntMultibandArray(f1)
    for(j<- 0 to f1.rows-1){
      for(i <- 0 to f1.cols-1){
        for(k <- 0 to f1.bandCount-1){
          fpart.band(k).asInstanceOf[IntRawArrayTile].set(i,j,f2.band(k).get(flessCols/2+i,flessRows/2+j))
        }
      }
    }

    println("Focal"+validate.getPercentualFitting(fpart,f1))
    println("SOH-Focal"+SoH.getSoHDowAndUp(fpart,f1).getDownUpString())
    settings = MetrikValidation.getBasicSettings(5,1,10,2,4,1,1)
    settings.focal = false
    writeBand(settings, export)
    val g2 = export.getMulitGeoTiff(settings,TifType.Cluster)
    settings = MetrikValidation.getBasicSettings(5,1,10,2,4,1,3)
    settings.focal = false
    writeBand(settings, export)
    val g1 = export.getMulitGeoTiff(settings,TifType.Cluster)

    val glessRows = g2.rows-g1.rows
    val glessCols = g2.cols-g1.cols
    val part = MultibandUtils.getEmptyIntMultibandArray(g1)
    for(j<- 0 to g1.rows-1){
      for(i <- 0 to g1.cols-1){
        for(k <- 0 to g1.bandCount-1){
          part.band(k).asInstanceOf[IntRawArrayTile].set(i,j,g2.band(k).get(glessCols/2+i,glessRows/2+j))
        }
      }
    }

    println("GStar"+validate.getPercentualFitting(part,g1))
    println("SOH-Gstar"+SoH.getSoHDowAndUp(part,g1).getDownUpString())
  }

  def writeBand(settings : Settings, importer : ImportGeoTiff): Unit = {
    println(PathFormatter.getDirectoryAndName(settings, TifType.Cluster))
   if (PathFormatter.exist(settings, TifType.Cluster)) {
      return
    }
    val transform = new Transformation()
    val mulitBand = transform.transformCSVtoTimeRaster(settings)
    importer.writeMultiGeoTiff(mulitBand, settings, TifType.Raw)
    val rdd = importer.repartitionFiles(settings)
    val gStar = TimeGetisOrd.getGetisOrd(rdd,settings,mulitBand)
    importer.writeMultiGeoTiff(gStar, settings, TifType.GStar)
    val hotspot = new ClusterHotSpotsTime(gStar)
    importer.writeMultiGeoTiff(hotspot.findClusters(), settings, TifType.Cluster)
  }
}
