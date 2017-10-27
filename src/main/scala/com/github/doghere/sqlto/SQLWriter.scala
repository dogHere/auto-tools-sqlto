package com.github.doghere.sqlto

import java.io.{File, FileWriter}
import java.sql.{Connection, DriverManager, ResultSet, Timestamp}

import com.norbitltd.spoiwo.model._
import com.norbitltd.spoiwo.model.enums.CellFill
import org.apache.commons.csv.{CSVFormat, CSVPrinter}
import com.norbitltd.spoiwo.natures.xlsx.Model2XlsxConversions._
import Tool._
import org.rosuda.REngine.REXP

import scala.collection.mutable.ListBuffer
import scala.xml.NodeSeq

object SQLWriter{

  def main(args: Array[String]): Unit = {
    val connection = getConnection("local")
    println(exists(connection,"len_match_trade"))
  }


  implicit class RWriter(val result:REXP){
    def toCSV(filename:String ):Boolean={
      val fileWriter = new FileWriter(filename)
      val csvFilePrinter = new CSVPrinter(fileWriter, CSVFormat.MYSQL)
      val header = result._attr.asList

      val res = header.at(0)
      csvFilePrinter.printRecord({
          if (res.isNumeric) res.asDoubles().toList.map(_.toString)
          else res.asStrings().toList
        }:_*
      )

      val columns  = result.asList()
      val cs = (0 until columns.size()).map(i=>{
        if (columns.at(i).isNumeric){
          columns.at(i).asDoubles().toList.map(_.toString)
        }else{
          columns.at(i).asStrings().toList.map(_.toString)
        }
      })
      if(cs.nonEmpty) {
        val rowSize = cs(0).size
        (0 until rowSize).foreach(i=>{
            csvFilePrinter.printRecord((0 until columns.size()).map {k=>
              cs(k)(i)
            }.toList:_*
          )
        })
      }


      csvFilePrinter.flush()
      csvFilePrinter.close()
      true
    }


    def toExcel(outputFilepath: String,sheetName:String="sheet1"): Boolean = {
      val headerStyle =
        CellStyle(fillPattern = CellFill.Solid,
          fillForegroundColor = Color.LightBlue,
          fillBackgroundColor = Color.LightBlue,
          font = Font(bold = true))

      val header = result._attr.asList
      val res = header.at(0)


      val sheet: Sheet = Sheet(name = sheetName)
        .withRows(
          com.norbitltd.spoiwo.model.Row(style = headerStyle).withCellValues(
            if (res.isNumeric) res.asDoubles().toList.map(_.toString)
            else res.asStrings().toList)
            :: {
              val columns  = result.asList()
              val cs = (0 until columns.size()).map(i=>{
                if (columns.at(i).isNumeric){
                  columns.at(i).asDoubles().toList.map(m=>if(m.isNaN)""else m)
                }else{
                  columns.at(i)
                    .asStrings()
                    .toList.map(m=>if(m==null)""else m)
                }
              })
              val rowSize = cs(0).size
              (0 until rowSize).map(i=>{
                Row().withCellValues(
                  (0 until columns.size()).map {k=>
                    cs(k)(i)
                  }.toList:_*
                )
              }).toList
            }
        )
      sheet.saveAsXlsx(outputFilepath)
      true
    }
  }


  implicit class Table(val resultSet: ResultSet) {
    def toCSV(filename: String): Boolean = {
//      import org.apache.commons.csv.CSVParser
//      csvFileParser = new CSVParser(fileReader, csvFileFormat)
      val fileWriter = new FileWriter(filename)
      val csvFilePrinter = new CSVPrinter(fileWriter, CSVFormat.MYSQL)
      val metaData = resultSet.getMetaData
      csvFilePrinter.printRecord(getHeader(resultSet):_*)
      csvFilePrinter.printRecords(resultSet)
      csvFilePrinter.flush()
      csvFilePrinter.close()
      true
    }

    def toHtml(filename:String):Boolean = {
      val writer = new FileWriter(filename)
      val header = getHeader(resultSet)

      val t =
        <div>
          <style>{
            """
                   table {
                     font-family: arial, sans-serif;
                     border-collapse: collapse;
                     width: 100%;
                   }

                   td, th {
                     border: 1px solid #dddddd;
                     text-align: left;
                     padding: 8px;
                   }

                   tr:nth-child(even) {
                      background-color: #dddddd;
                   }
            """.stripMargin}

        </style>

          <table border="1" cellspacing="0" cellpadding="0" >
          {<tr>{header.map(k=> <td><center>{k}</center></td>)}</tr>}
          {
            val count = resultSet.getMetaData.getColumnCount

            val l = ListBuffer[NodeSeq]()
            while (resultSet.next()){
              l+=
              <tr>{
              (1 to count).map(i => {
                val v = resultSet.getObject(i)
               <td><center>{if(v==null)""else v} </center></td>
              })}
              </tr>
            }
            l
          }

        </table>
        </div>

      writer.write(t.toString())
      writer.flush()
      writer.close()
      true
    }

    def toExcel(outputFilepath: String,sheetName:String="sheet1"): Boolean = {

      val headerStyle =
        CellStyle(fillPattern = CellFill.Solid,
          fillForegroundColor = Color.LightBlue,
          fillBackgroundColor = Color.LightBlue,
          font = Font(bold = true))

      val sheet: Sheet = Sheet(name = sheetName)
        .withRows(
          com.norbitltd.spoiwo.model.Row(style = headerStyle).withCellValues(getHeader(resultSet))
           :: {
            val count = resultSet.getMetaData.getColumnCount
            new Iterator[Row] {
              def hasNext: Boolean = resultSet.next()
              def next(): Row = {
                val count = resultSet.getMetaData.getColumnCount
                Row().withCellValues(
                  (1 to count).map(i=> {
                      val v = resultSet.getObject(i)
                      val t = resultSet.getMetaData.getColumnClassName(i)
                      if (v == null) {
                        ""
                      }else{
                        if (t=="java.math.BigDecimal")
                          v.asInstanceOf[java.math.BigDecimal].doubleValue()
                        else{
                          v
                        }
                      }
                    }
                  ).toList
                )
              }
            }.toList
          }
        )
      
      sheet.saveAsXlsx(outputFilepath)
      true
    }



    def toTable(connection: Connection,tableName:String,ifCreate:Boolean=true,cacheOnce:Boolean=false):Boolean={

      val tableExists = exists(connection,tableName)

      if(tableExists && cacheOnce){
        return true
      }

      val dbName = connection.getMetaData.getDatabaseProductName
//      println(dbName)
      val header = getHeader(resultSet)
      val className = {
        if(dbName=="SQLite") {
          getClassNames(resultSet).map {
            case "java.lang.Integer" => "int"
            case "java.lang.Double" | "java.lang.Float" | "java.math.BigDecimal" => "double"
            case "java.lang.String" => "text"
            case "java.sql.Timestamp" | "java.sql.Time" | "java.sql.Date" => "timestamp"
            case _ => "text"
          }
        }else if(dbName=="PostgreSQL"){
          getClassNames(resultSet).map {
            case "java.lang.Integer" => "int"
            case "java.lang.Double" | "java.lang.Float" | "java.math.BigDecimal" => "decimal"
            case "java.lang.String" => "text"
            case "java.sql.Timestamp" | "java.sql.Time" | "java.sql.Date" => "timestamp"
            case _ => "text"
          }
        }else{
          getClassNames(resultSet).map {
            case "java.lang.Integer" => "int"
            case "java.lang.Double" | "java.lang.Float" | "java.math.BigDecimal" => "double"
            case "java.lang.String" => "text"
            case "java.sql.Timestamp" | "java.sql.Time" | "java.sql.Date" => "timestamp"
            case _ => "text"
          }
        }
      }



      if(ifCreate) {
        val drop = s"""drop table if exists $tableName"""
        val create = s"""create table $tableName (${header.zip(className).map(t =>  t._1 + " " + t._2).mkString(",\n")})"""
        val statement = connection.createStatement()
        statement.execute(drop)
        statement.execute(create)
      }
      connection.setAutoCommit(false)
      val insert = s"""insert into $tableName (${header.mkString(",")}) values (${header.map(k=>"?").mkString(",")})"""
      val ps = connection.prepareStatement(insert)

      val count = resultSet.getMetaData.getColumnCount

      var countExec = 0

      while (resultSet.next()) {
        (1 to count).foreach(i => {
          val v = resultSet.getObject(i)
          val t = resultSet.getMetaData.getColumnClassName(i)

          if(t=="oracle.sql.TIMESTAMP" && v!=null){
            ps.setTimestamp(i,v.asInstanceOf[oracle.sql.TIMESTAMP].timestampValue())
          }else {
            ps.setObject(i, v)
          }
        })
        ps.addBatch()
        countExec += 1
        if(countExec%3000==0){
          ps.executeBatch()
        }
      }
      ps.executeBatch()
      connection.commit()
      ps.close()
      connection.setAutoCommit(true)
      true
    }
  }
}