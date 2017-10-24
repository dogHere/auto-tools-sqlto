package com.github.doghere.sqlto

import java.io.{File, FileFilter, FileInputStream}
import java.sql.{Connection, DriverManager, ResultSet}


object Tool{
  def getHeader(resultSet: ResultSet): List[String] ={
    val meta = resultSet.getMetaData
    val count = meta.getColumnCount
    (1 to count).map(i=>{
      meta.getColumnName(i)
    }).toList
  }

  def getClassNames(resultSet: ResultSet):List[String]={
    val meta = resultSet.getMetaData
    val count = meta.getColumnCount
    (1 to count).map(i=>{
      meta.getColumnClassName(i)
    }).toList
  }



  def getConnection(name:String): Connection ={
    val dbName = name
    val config = scala.xml.XML
      .load(System.getProperty("user.home") + "/.dbconfig/dbconfig.xml")
      .\("db").filter(v => v.attribute("name").getOrElse().toString == dbName)
      .head

    val url = config \ "url" text
    val username = config \ "username" text
    val password = config \ "password" text
    val connection: Connection =
      DriverManager.getConnection(url, username, password)
    connection
  }

  def exists(connection: Connection,tableName:String):Boolean={
//    val dbName = connection.getMetaData.getDatabaseProductName

    val table = if(tableName.contains(".")){
      tableName.split('.')(1)
    }else{
      tableName
    }

    val dbm = connection.getMetaData
    // check if "employee" table is there
    val tables = dbm.getTables(null, null, table, null)
    if (tables.next) {
      true
    } else {
      false
    }
  }
}