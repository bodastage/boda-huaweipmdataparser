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
import scopt.OParser
import java.util.zip.GZIPInputStream
import java.io.BufferedInputStream
import java.io.FileInputStream

case class Config(
                   in: File = new File("."))

object BodaHuaweiPMDataParser{
  def main(args: Array[String]): Unit = {

    val builder = OParser.builder[Config]
    val parser1 = {
      import builder._
      OParser.sequence(
        programName("boda-huaweipmdataparser"),
        head("boda-huaweipmdataparser", "0.0.3"),
        opt[File]('i', "in")
          .required()
          .valueName("<file>")
          .action((x, c) => c.copy(in = x))
          .validate(f =>
            if( (!Files.isRegularFile(f.toPath) && !Files.isDirectory(f.toPath))
              && !Files.isReadable(f.toPath)) failure(s"Failed to access input file/directory called ${f.getName}")
            else success
          )
          .text("input file or directory, required."),
        help("help").text("prints this usage text"),
        note(sys.props("line.separator")),
        note("Parses Huawei performance management XML files to csv. It processes plain text XML and gzipped XML files."),
        note("Examples:"),
        note("java -jar boda-huaweipmdataparser.jar -i FILENAME.xml"),
        note("java -jar boda-huaweipmdataparser.jar -i FILENAME.gz"),
        note(sys.props("line.separator")),
        note("Copyright (c) 2019 Bodastage Solutions(http://www.bodastage.com)")

      )
    }

    var inputFile : String = ""
    OParser.parse(parser1, args, Config()) match {
      case Some(config) =>
        inputFile = config.in.getAbsolutePath
      case _ =>
        sys.exit(1)
    }

    try{

      println("filename,start_time,file_format_version,vendor_name,element_type,managed_element,meas_info_id,gran_period_duration,gran_period_endtime,rep_period_duration,meas_obj_ldn,counter_id,counter_value")

      this.processFileOrDirectory(inputFile)

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

    val contentType = Files.probeContentType(Paths.get(fileName))

    var xml = new XMLEventReader(Source.fromFile(fileName))

    if(contentType == "application/x-gzip"){
      xml = new XMLEventReader(Source.fromInputStream(this.getGZIPInputStream(fileName)))
    }

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

  /**
    * Returns InputFileFrom
    *
    * @param s
    * @return
    */
  def getGZIPInputStream(s: String) = new GZIPInputStream(new BufferedInputStream(new FileInputStream(s)))

}