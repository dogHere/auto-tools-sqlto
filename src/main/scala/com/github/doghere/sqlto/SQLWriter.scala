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
    private var isRead = false
    def toCSV(filename: String): Boolean = {


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
          <meta charset="utf-8"/>
          <style type="text/css">{
            """
         /***********
                       Originally based on The MailChimp Reset from Fabio Carneiro, MailChimp User Experience Design
                       More info and templates on Github: https://github.com/mailchimp/Email-Blueprints
                       http://www.mailchimp.com &amp; http://www.fabio-carneiro.com
                       INLINE: Yes.
                       ***********/
                       /* Client-specific Styles */
                       #outlook a {padding:0;} /* Force Outlook to provide a "view in browser" menu link. */
                       body{width:100% !important; -webkit-text-size-adjust:100%; -ms-text-size-adjust:100%; margin:0; padding:0;}
                       /* Prevent Webkit and Windows Mobile platforms from changing default font sizes, while not breaking desktop design. */
                       .ExternalClass {width:100%;} /* Force Hotmail to display emails at full width */
                       .ExternalClass, .ExternalClass p, .ExternalClass span, .ExternalClass font, .ExternalClass td, .ExternalClass div {line-height: 100%;} /* Force Hotmail to display normal line spacing.  More on that: http://www.emailonacid.com/forum/viewthread/43/ */
                       #backgroundTable {margin:0; padding:0; width:80% !important; line-height: 100% !important;}
                       /* End reset */
                       /* Some sensible defaults for images
                       1. "-ms-interpolation-mode: bicubic" works to help ie properly resize images in IE. (if you are resizing them using the width and height attributes)
                       2. "border:none" removes border when linking images.
                       3. Updated the common Gmail/Hotmail image display fix: Gmail and Hotmail unwantedly adds in an extra space below images when using non IE browsers. You may not always want all of your images to be block elements. Apply the "image_fix" class to any image you need to fix.
                       Bring inline: Yes.
                       */
                       img {outline:none; text-decoration:none; -ms-interpolation-mode: bicubic;}
                       a img {border:none;}
                       .image_fix {display:block;}
                       /** Yahoo paragraph fix: removes the proper spacing or the paragraph (p) tag. To correct we set the top/bottom margin to 1em in the head of the document. Simple fix with little effect on other styling. NOTE: It is also common to use two breaks instead of the paragraph tag but I think this way is cleaner and more semantic. NOTE: This example recommends 1em. More info on setting web defaults: http://www.w3.org/TR/CSS21/sample.html or http://meiert.com/en/blog/20070922/user-agent-style-sheets/
                       Bring inline: Yes.
                       **/
                       p {margin: 1em 0;}
                       /** Hotmail header color reset: Hotmail replaces your header color styles with a green color on H2, H3, H4, H5, and H6 tags. In this example, the color is reset to black for a non-linked header, blue for a linked header, red for an active header (limited support), and purple for a visited header (limited support).  Replace with your choice of color. The !important is really what is overriding Hotmail's styling. Hotmail also sets the H1 and H2 tags to the same size.
                       Bring inline: Yes.
                       **/
                       h1, h2, h3, h4, h5, h6 {color: black !important;}
                       h1 a, h2 a, h3 a, h4 a, h5 a, h6 a {color: blue !important;}
                       h1 a:active, h2 a:active,  h3 a:active, h4 a:active, h5 a:active, h6 a:active {
                       color: red !important; /* Preferably not the same color as the normal header link color.  There is limited support for psuedo classes in email clients, this was added just for good measure. */
                       }
                       h1 a:visited, h2 a:visited,  h3 a:visited, h4 a:visited, h5 a:visited, h6 a:visited {
                       color: purple !important; /* Preferably not the same color as the normal header link color. There is limited support for psuedo classes in email clients, this was added just for good measure. */
                       }
                       /** Outlook 07, 10 Padding issue: These "newer" versions of Outlook add some padding around table cells potentially throwing off your perfectly pixeled table.  The issue can cause added space and also throw off borders completely.  Use this fix in your header or inline to safely fix your table woes.
                       More info: http://www.ianhoar.com/2008/04/29/outlook-2007-borders-and-1px-padding-on-table-cells/
                       http://www.campaignmonitor.com/blog/post/3392/1px-borders-padding-on-table-cells-in-outlook-07/
                       H/T @edmelly
                       Bring inline: No.
                       **/
                       table td {border-collapse: collapse;}
                       /** Remove spacing around Outlook 07, 10 tables
                       More info : http://www.campaignmonitor.com/blog/post/3694/removing-spacing-from-around-tables-in-outlook-2007-and-2010/
                       Bring inline: Yes
                       **/
                       table { border-collapse:collapse; mso-table-lspace:0pt; mso-table-rspace:0pt; }
                       /* Styling your links has become much simpler with the new Yahoo.  In fact, it falls in line with the main credo of styling in email, bring your styles inline.  Your link colors will be uniform across clients when brought inline.
                       Bring inline: Yes. */
                       a {color: orange;}
                       /* Or to go the gold star route...
                       a:link { color: orange; }
                       a:visited { color: blue; }
                       a:hover { color: green; }
                       */
                       /***************************************************
                       ****************************************************
                       MOBILE TARGETING
                       Use @media queries with care.  You should not bring these styles inline -- so it's recommended to apply them AFTER you bring the other stlying inline.
                       Note: test carefully with Yahoo.
                       Note 2: Don't bring anything below this line inline.
                       ****************************************************
                       ***************************************************/
                       /* NOTE: To properly use @media queries and play nice with yahoo mail, use attribute selectors in place of class, id declarations.
                       table[class=classname]
                       Read more: http://www.campaignmonitor.com/blog/post/3457/media-query-issues-in-yahoo-mail-mobile-email/
                       */
                       @media only screen and (max-device-width: 480px) {
                       /* A nice and clean way to target phone numbers you want clickable and avoid a mobile phone from linking other numbers that look like, but are not phone numbers.  Use these two blocks of code to "unstyle" any numbers that may be linked.  The second block gives you a class to apply with a span tag to the numbers you would like linked and styled.
                       Inspired by Campaign Monitor's article on using phone numbers in email: http://www.campaignmonitor.com/blog/post/3571/using-phone-numbers-in-html-email/.
                       Step 1 (Step 2: line 224)
                       */
                       a[href^="tel"], a[href^="sms"] {
             text-decoration: none;
             color: black; /* or whatever your want */
             pointer-events: none;
             cursor: default;
           }
             .mobile_link a[href^="tel"], .mobile_link a[href^="sms"] {
             text-decoration: default;
             color: orange !important; /* or whatever your want */
             pointer-events: auto;
             cursor: default;
           }
           }
           /* More Specific Targeting */
           @media only screen and (min-device-width: 768px) and (max-device-width: 1024px) {
           /* You guessed it, ipad (tablets, smaller screens, etc) */
           /* Step 1a: Repeating for the iPad */
           a[href^="tel"], a[href^="sms"] {
             text-decoration: none;
             color: blue; /* or whatever your want */
             pointer-events: none;
             cursor: default;
           }
             .mobile_link a[href^="tel"], .mobile_link a[href^="sms"] {
             text-decoration: default;
             color: orange !important;
             pointer-events: auto;
             cursor: default;
           }
           }
           @media only screen and (-webkit-min-device-pixel-ratio: 2) {
           /* Put your iPhone 4g styles in here */
           }
           /* Following Android targeting from:
           http://developer.android.com/guide/webapps/targeting.html
           http://pugetworks.com/2011/04/css-media-queries-for-targeting-different-mobile-devices/  */
           @media only screen and (-webkit-device-pixel-ratio:.75){
           /* Put CSS for low density (ldpi) Android layouts in here */
           }
           @media only screen and (-webkit-device-pixel-ratio:1){
           /* Put CSS for medium density (mdpi) Android layouts in here */
           }
           @media only screen and (-webkit-device-pixel-ratio:1.5){
           /* Put CSS for high density (hdpi) Android layouts in here */
           }
           /* end Android targeting */
            """.stripMargin}

        </style>

          <table border="0" cellspacing="0" cellpadding="0"  id="backgroundTable" bgcolor="#EEF3F6" style="border:3px"  rules="all">
          {<tr>{header.map(k=> <th bgcolor="#BEDBFA"><center>{k}</center></th>)}</tr>}
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
      while(resultSet.next()){
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