package exportstructure

import geotrellis.raster.Tile
import getisOrd.SoH.SoHR
import geotrellis.Weight.Weight
import parmeters.Settings

/**
  * Created by marc on 11.05.17.
  */
class SoHResult(parent : Tile, weight : Tile, wParent : Settings, time : Long, sohValue : SoHR, lat : Int) {

  def copySettings(): Settings = {
    val set = new Settings
    set.sizeOfRasterLat = wParent.sizeOfRasterLat
    set.focal = wParent.focal
    set.focalRange = wParent.focalRange
    set.weightMatrix = wParent.weightMatrix
    set.weightRadius = wParent.weightRadius
    set.aggregationLevel = wParent.aggregationLevel
    set.hour = wParent.hour
    set
  }

  val localSet = copySettings()

  def format(shortFormat : Boolean): String = {
    if(shortFormat){
      return formatShort()
    }
    val parentString  = lat+","+localSet.hour+","+localSet.focal+","+localSet.focalRange+","+parent.cols+","+parent.rows+","+localSet.weightMatrix+","+localSet.weightRadius
    //val childString = wChild.sizeOfRasterLat+","+wChild.focal+","+wChild.weightMatrix+","+wChild.weightRadius
    return parentString+","+time+","+sohValue.getDown()+","+sohValue.getUp()

  }

  def formatShort(): String = {
    return getLat+","+localSet.hour+","+sohValue.getUp()+","+sohValue.getDown()
  }

  def headerShort() : String = {
    "rasterSize(meters),time,downward-"+localSet.focal+"-"+localSet.focalRange+",upward-"+localSet.focal+"-"+localSet.focalRange
  }

  def getLat() : Int = {
    lat
  }

  def header(shortFormat : Boolean): String ={
    if(shortFormat){
      return headerShort()
    }
    "rasterSize(meters),time,parentFocal,focalRange,cols,rows,weighParent,weightParentRadius,duration(seconds),downward"+wParent.focal+"-"+wParent.focalRange+",upward"+wParent.focal+"-"+wParent.focalRange+","
  }

  def getSohUp(): Double = {
    sohValue.getUp()
  }

  def getSohDown(): Double = {
    sohValue.getDown()
  }






}
