package com.bodastage.huawei

import scala.io.Source
import scala.xml.pull._
import scala.collection.mutable.ArrayBuffer
import java.io.{BufferedInputStream, File, FileInputStream, PrintWriter}
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.Files

import scala.xml.XML
import scala.collection.mutable.ListBuffer
import scopt.OParser
import java.util.zip.GZIPInputStream

case class Config(
                   in: File = new File("."),
                   out: File = null,
                   version : Boolean = false
                 )

object BodaHuaweiPMDataParser{
  var outputFolder: String = "";

  def main(args: Array[String]): Unit = {

    val builder = OParser.builder[Config]
    val parser1 = {
      import builder._
      OParser.sequence(
        programName("boda-huaweipmdataparser"),
        head("boda-huaweipmdataparser", "0.1.1"),
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
        opt[File]('o', "out")
          .valueName("<file>")
          .action((x, c) => c.copy(out = x))
          .validate(f =>
            if( !Files.isDirectory(f.toPath ) && !Files.isReadable(f.toPath)) failure(s"Failed to access outputdirectory called ${f.getName}")
            else success
          )
          .text("output directory required."),
        opt[Unit]('v', "version")
          .action((_, c) => c.copy(version = true))
          .text("Show version"),
        help("help").text("prints this usage text"),
        note(sys.props("line.separator")),
        note("Parses Huawei performance management XML files to csv. It processes plain text XML and gzipped XML files."),
        note("Examples:"),
        note("java -jar boda-huaweipmdataparser.jar -i FILENAME.xml"),
        note("java -jar boda-huaweipmdataparser.jar -i FILENAME.gz"),
        note("java -jar boda-huaweipmdataparser.jar -i FILENAME.gz -o outputFolder"),
        note("java -jar boda-huaweipmdataparser.jar -i FILENAME.xml -o outputFolder"),
        note(sys.props("line.separator")),
        note("Copyright (c) 2019 Bodastage Solutions(http://www.bodastage.com)")

      )
    }

    var inputFile : String = ""
    var outFile : File = null;
    var showVersion : Boolean = false;
    OParser.parse(parser1, args, Config()) match {
      case Some(config) =>
        inputFile = config.in.getAbsolutePath
        outFile = config.out
        showVersion = config.version
      case _ =>
        sys.exit(1)
    }

    if(showVersion){
      println("0.1.1")
      sys.exit(0);
    }

    try{

      if(outFile != null) outputFolder = outFile.getAbsoluteFile().toString

      if(outputFolder.length == 0){
        val header : String = "filename," +
          "collection_begin_time," +
          "collection_end_time," +
          "file_format_version," +
          "vendor_name," +
          "element_type," +
          "managed_element," +
          "meas_infoid," +
          "gran_period_duration," +
          "gran_period_endtime," +
          "rep_period_duration," +
          "meas_objldn," +
          "counter_id," +
          "counter_value," +
          "suspect";
        println(header)
      }else{
        outputFolder = outFile.getAbsolutePath();
      }


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
    * Extact the measurement collection start and end time
    *
    * @param fileName
    *
    * @return array [startTime, endTime]
    */
  def extractMeasCollectTime(fileName : String) : Array[String] = {
    val contentType = Files.probeContentType(Paths.get(fileName))

    var xml = new XMLEventReader(Source.fromFile(fileName))

    if(contentType == "application/x-gzip"){
      xml = new XMLEventReader(Source.fromInputStream(this.getGZIPInputStream(fileName)))
    }

    var beginTime:String = "";
    var endTime:String = "";

    var measCollectionTime:Array[String] = new Array[String](2)

    for(event <- xml) {
      event match {
        case EvElemStart(_, tag, attrs, _) => {
          if (tag == "measCollec") {
            for (m <- attrs) {
              if (m.key == "beginTime") beginTime = m.value.toString()
              if (m.key == "endTime") endTime = m.value.toString()
            }
          }
        }
        case _ =>
      }
    }

    measCollectionTime(0) = beginTime;
    measCollectionTime(1) = endTime;

    return measCollectionTime;
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
    var suspect: String = ""

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

    //Get collection time
    var measCollectionTime:Array[String] = new Array[String](2);
    measCollectionTime = extractMeasCollectTime(fileName)

    val fileBaseName: String  = getFileBaseName(fileName);
    var pw : PrintWriter = null;
    if(outputFolder.length > 0){

      val csvFile : String = outputFolder + File.separator + fileBaseName.replaceAll(".(xml|gz)$",".csv");

      pw  = new PrintWriter(new File(csvFile));
      val header : String =
        "file_name," +
        "collection_begin_time," +
        "collection_end_time," +
        "file_format_version," +
        "vendor_name," +
        "element_type," +
        "managed_element," +
        "meas_infoid," +
        "gran_period_duration," +
        "gran_period_endtime," +
        "rep_period_duration," +
        "meas_objldn," +
        "counter_id," +
        "counter_value," +
        "suspect";
      pw.write(header + "\n");
    }

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
          if (tag == "suspect") {
            suspect = buf.mkString
          }

          if (tag == "measTypes") {
            val msTypes = buf.mkString.replaceAll("\\s+"," ").trim().split(" ")
            measTypes = ListBuffer(msTypes: _ *)
          }


          if (tag == "measResults") {
            val msResults = buf.mkString.replaceAll("\\s+"," ").trim().split(" ")
            for((v,i) <- msResults.view.zipWithIndex){
              val counterVal : String = v
              val counterId: String = measTypes(i)

              val csvRow : String =
                s"${toCSVFormat(getFileBaseName(fileName))}," +
                s"$startTime," +
                s"${measCollectionTime(1)}," +
                s"$fileFormatVersion," +
                s"${toCSVFormat(vendorName)}," +
                s"${toCSVFormat(elementType)}," +
                s"${toCSVFormat(managedElementUserLabel)}," +
                s"$measInfoId," +
                s"$granPeriodDuration," +
                s"$granPeriodEndTime," +
                s"$repPeriodDuration," +
                s"${measObjLdn}," +
                s"${counterId}," +
                s"${counterVal}," +
                s"${suspect}";

              if(outputFolder.length == 0) {
                println(csvRow);
              }else{
                pw.write(csvRow + "\n");
              }
            }
          }

          buf.clear
        }

        case _ =>
      }
    }

    if(pw != null ) pw.close();
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