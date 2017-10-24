package com.github.doghere.sqlto

import java.sql.{Connection, ResultSet, SQLException, Statement}

import scala.language.implicitConversions

object SQLReader {


  def use(connection: Connection)(codeBlock: SQLReader => Unit): Unit = {
    if (connection == null) {
      System.err.println("Connection is null")
    } else if (connection.isClosed) {
      System.err.println("Connection is closed")
    } else {
      val reader = new SQLReader(Some(connection))
      try {
        codeBlock(reader)
      } finally {
        reader.statement match {
          case Some(s) => s.close()
          case _ =>
        }
        connection.close()
      }
    }
  }

}


class SQLReader private(val connection: Option[Connection]) {

  private lazy val statement: Option[Statement] = {
    connection match {
      case None =>
        System.err.println("Connection is None")
        None
      case Some(con) =>
        if (con == null) {
          System.err.println("Connection is null")
          None
        } else if (con.isClosed) {
          System.err.println("Connection is closed")
          None
        } else {
          // streams
          val st = con.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY,
            java.sql.ResultSet.CONCUR_READ_ONLY)
//          st.setFetchSize(Integer.MIN_VALUE)
          Some(st)
        }

    }
  }


  def fetch(sql:String): Option[ResultSet] = {
    val resultSet= {
      statement match {
        case None =>
          System.err.println("statement is None")
          None
        case Some(stat) =>
          if (stat == null) {
            System.err.println("statement is null")
            None
          } else if (stat.isClosed) {
            System.err.println("statement is closed")
            None
          } else {
            Some(stat.executeQuery(sql))
          }
      }
    }
    resultSet
  }

  def update(sql:String):Boolean = {
    statement match {
      case None =>
        System.err.println("statement is None")
        false
      case Some(stat) =>
        if (stat == null) {
          System.err.println("statement is null")
          false
        } else if (stat.isClosed) {
          System.err.println("statement is closed")
          false
        } else {
          connection.get.setAutoCommit(false)
          var res = false
          try {
            res = stat.execute(sql)
            connection.get.commit()
          }catch {
            case e:SQLException => connection.get.rollback()
              throw new SQLException(e)
          }finally {
            connection.get.setAutoCommit(true)
          }
          res
        }
    }
  }


}