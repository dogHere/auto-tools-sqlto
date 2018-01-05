# sqlto

A tool to read data from database then write data into database,html,csv,excel .

# Example

```scala
import com.github.doghere.sqlto._

SQLReader.use(cacheConnection)(cacheReader => {
      cacheReader.fetch("select username,user_group from user") match {
          case None => println("nothing")
          case Some(resultSet) => {
              // do something ... 
              resultSet.toTable(memConnection,"user_tmp")
              // select from memory
              SQLReader.use(memConnection)(memReader=>{
                 // deal with memReader
                 memReader.fetch("""
                 select user_group,count(username) as user_count
                 from user_tmp 
                 group by user_group
                 """)
              })
          }
      }
    }
）

```


