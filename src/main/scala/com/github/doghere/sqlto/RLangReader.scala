package com.github.doghere.sqlto

import org.rosuda.REngine._
import org.rosuda.REngine.Rserve._


object RLangReader{

  def use(connection: RConnection)(codeBlock: RLangReader => Unit): Unit = {
    if (connection == null) {
      System.err.println("Connection is null")
    } else if (!connection.isConnected) {
      System.err.println("Connection is closed")
    } else {
      val reader = new RLangReader(Some(connection))
      try {
        codeBlock(reader)
      } finally {
        connection.close()
      }
    }
  }


  def main(args: Array[String]): Unit = {
    import SQLWriter._
    val c = new RConnection()
    val x =
      """
	| Some R code
      """.stripMargin
    use(c){reader=>{
      reader.fetch(x) match {
        case None=>
        case Some(rexp)=>{
          rexp.toExcel("save path")
        }
      }
    }}
  }
}

class RLangReader(val connection:Option[RConnection]){
  def fetch(rScript:String): Option[REXP] ={
    connection match {
      case None=>None
      case Some(conn)=>
        Some(conn.eval(rScript))
    }
  }
}
