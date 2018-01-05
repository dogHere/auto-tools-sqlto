# sqlto

A tool to read data from database then write data into database,html,csv,excel .

# Example

```scala
import com.github.doghere.sqlto._

SQLReader.use(cacheConnection)(cacheReader => {
  reader.fetch("select * from dual") match 
  {
      case None => println("nothing")
      case Some(resultSet) => {
          // do something ... 
          resultSet.toTable(memConnection,"table name")
          // select from memory
          SQLReader.use(cacheConnection)(memReader=>{
            //select 
          })
      }
  }
}

```


