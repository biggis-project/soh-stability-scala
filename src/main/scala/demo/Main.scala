package demo

import com.typesafe.scalalogging.LazyLogging
import importExport.DownloadFilesFromWeb
import parmeters.Settings
import scenarios._

/**
  * Used to run scenarios which use [[getisOrd.GetisOrd]] and [[getisOrd.GetisOrdFocal]].
  */
object Main extends App with LazyLogging {
    val downloader = new DownloadFilesFromWeb()
    val setting = new Settings()
    setting.csvMonth = 1
    setting.csvYear = 2016
    downloader.downloadNewYorkTaxiFiles(setting)

    var scenario : GenericScenario = new DifferentRatio()
    scenario.runScenario()
    scenario = new DifferentRasterSizes()
    scenario.runScenario()
    scenario   = new DifferentFocal()
    scenario.runScenario()

}
