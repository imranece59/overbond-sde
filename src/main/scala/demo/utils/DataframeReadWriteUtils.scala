package demo.utils

import org.apache.spark.sql.hive._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql._
import org.apache.spark.sql.SaveMode

object DataframeReadWriteUtils {
  
        /**
       	*  create dataframe from JSON
       	*/
        def creatingDataframeFromJson(sparkSession : SparkSession, filePath : String):DataFrame={
        sparkSession.read.option("multiLine", true).json(filePath)}
       
        /**
        *  dataframe persist
        */
        def dataframepersist(dataframe: DataFrame) ={
         dataframe.persist(StorageLevel.MEMORY_AND_DISK_SER)}
       
        /**
        *  dataframe unpersist
        */ 
        def dataframeunpersist(dataframe:DataFrame) ={
         dataframe.unpersist()}
        
        /**
        *  dataset write to path
        */ 
        def dataSetWrite(ds:Dataset[String],mode:String,format:String,outputPath:String) ={
         ds.write.format(format).mode(mode).save(outputPath)
//          ds.write.json(outputPath)
         }
        
        
}



