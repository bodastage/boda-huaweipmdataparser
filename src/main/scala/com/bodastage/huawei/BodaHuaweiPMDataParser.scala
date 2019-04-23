package com.bodastage.huawei

import scala.io.Source
import scala.xml.pull._
import scala.collection.mutable.ArrayBuffer
import java.io.File
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.Files
import scala.xml.XML
import scala.collection.mutable.ListBuffer

object BodaHuaweiPMDataParser{
  def main(args: Array[String]): Unit = {

    if(args.length != 1){
      println("usage: java -jar boda-huaweipmdataparser.jar input_file")
      sys.exit(1)
    }

    try{
      val f : Path = Paths.get(args(0))
      if( (!Files.isRegularFile(f) && !Files.isDirectory(f)) && !Files.isReadable(f)){
        throw new Exception(args(0).toString)
      }

      println("filename,start_time,file_format_version,vendor_name,element_type,managed_element,meas_info_id,gran_period_duration,gran_period_endtime,rep_period_duration,meas_obj_ldn,counter_id,counter_value")

      this.processFileOrDirectory(args(0))

    }catch{
      case ex: Exception => {
        println("Error accessing file")
        sys.exit(1)
      }
    }

  }

  /**
    * Get file base name
    *
    * @param fileName
    * @return
    */
  def getFileBaseName(fileName: String): String ={
    try{
      return new File(fileName).getName
    }catch{
      case ex: Exception => {
        return fileName
      }
    }
  }

  def processFileOrDirectory(inputPath: String): Unit ={


    val file : Path = Paths.get(inputPath)
    val isRegularExecutableFile : Boolean = Files.isRegularFile(file) & Files.isReadable(file)
    val isReadableDirectory = Files.isDirectory(file) & Files.isReadable(file)

    if (isRegularExecutableFile) {
      this.parseFile(inputPath)
    }

    if (isReadableDirectory) {
      val directory = new File(inputPath)

      val fList = directory.listFiles
      for(f:File <- fList){
        this.parseFile(f.getAbsolutePath)
      }
    }

  }

  /**
    * Parse a file
    * @param filename
    */
  def parseFile(fileName: String) : Unit = {

    var fileFormatVersion: String = ""
    var vendorName: String = ""
    var elementType: String = ""
    var startTime: String = ""

    var managedElementUserLabel: String = "";
    var measInfoId: String = ""
    var granPeriodDuration: String = ""
    var granPeriodEndTime: String = ""
    var repPeriodDuration: String = ""

    var measTypes = new ListBuffer[String] // Counters
    var measObjLdn: String = ""
    var measResults: String = ""

    //    val outputDirectory = new File(args(1))

    val xml = new XMLEventReader(Source.fromFile(fileName))
    var buf = ArrayBuffer[String]()

    for(event <- xml) {
      event match {
        case EvElemStart(_, tag, attrs, _) => {
          buf.clear

          if (tag == "fileHeader") {
            for (m <- attrs) {
              if (m.key == "fileFormatVersion") fileFormatVersion = m.value.toString()
              if (m.key == "vendorName") vendorName = m.value.toString()
            }
          }


          if (tag == "fileSender") {
            for (m <- attrs) {
              if (m.key == "elementType") elementType = m.value.toString()
            }
          }

          if (tag == "measCollec") {
            for (m <- attrs) {
              if (m.key == "beginTime") startTime = m.value.toString()
            }
          }

          if (tag == "managedElement") {
            for (m <- attrs) {
              if (m.key == "userLabel") managedElementUserLabel = m.value.toString()
            }
          }

          if (tag == "measInfo") {
            for (m <- attrs) {
              if (m.key == "measInfoId") measInfoId = m.value.toString()
            }
          }

          if (tag == "granPeriod") {
            for (m <- attrs) {
              if (m.key == "duration") granPeriodDuration = m.value.toString()
              if (m.key == "endTime") granPeriodEndTime = m.value.toString()
            }
          }

          if (tag == "repPeriod") {
            for (m <- attrs) {
              if (m.key == "duration") repPeriodDuration = m.value.toString()
            }
          }

          if (tag == "measValue") {
            for (m <- attrs) {
              if (m.key == "measObjLdn") measObjLdn = this.toCSVFormat(m.value.toString())
            }
          }

        }
        case EvText(t) => {
          buf += (t)
        }

        case EvElemEnd(_, tag) => {
          if (tag == "measTypes") {
            val msTypes = buf.mkString.trim.split(" ")
            measTypes = ListBuffer(msTypes: _ *)
          }


          if (tag == "measResults") {
            val msResults = buf.mkString.trim.split(" ")
            for((v,i) <- msResults.view.zipWithIndex){
              val counterVal : String = v
              val counterId: String = measTypes(i)

              println(s"${getFileBaseName(fileName)},$startTime,$fileFormatVersion,$vendorName,$elementType,$managedElementUserLabel,$measInfoId,$granPeriodDuration,$granPeriodEndTime,$repPeriodDuration,$measObjLdn,$counterId,$counterVal")
            }

          }

          buf.clear
        }

        case _ =>
      }
    }
  }

  def toCSVFormat(s: String): String = {
    var csvValue: String = s

    if(s.matches(".*,.*")){
      csvValue = "\"" + s + "\""
    }

    if(s.matches(""".*".*""")){
      csvValue = "\"" + s.replace("\"", "\"\"") + "\""
    }

    return csvValue
  }

}

package object BodaHuaweiPMParser {

}
