package demo.helper

import org.apache.spark.sql.{DataFrame,Dataset}
import org.apache.spark.sql.Row
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.regexp_replace
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import demo.constants.Constants
import org.apache.spark.sql.expressions.Window
object ProcessDataHelper {
  
        /**
       	*  Flattening the json to single level
       	*/
        def flattenJsonColumns(df : DataFrame):DataFrame = {
           val flatJsonDf=df.select(explode(col("data"))).toDF("data")
           flatJsonDf.selectExpr("data.id","data.type","data.tenor","data.yield","data.amount_outstanding")              
        }
        
        /**
       	*  separate corporate and government records along with dropping the records where 
       	*  any of the property is null. 
       	*  calculate the best benchmark
       	*/
        def calculateBestbenchMarkDf(df: DataFrame): DataFrame = {
            val corporateDf= df.filter("type='corporate' and tenor is not null and yield is not null and amount_outstanding is not null")
            .selectExpr("id as corporate_bond_id","regexp_replace(tenor,'years','') as corporate_tenor",
                "regexp_replace(yield,'%','') as corporate_yield","amount_outstanding as corporate_amount_outstanding")
                
            val governmentDf= df.filter("type='government' and tenor is not null and yield is not null and amount_outstanding is not null")
            .selectExpr("id as government_bond_id","regexp_replace(tenor,'years','') as government_tenor",
                "regexp_replace(yield,'%','') as government_yield","amount_outstanding as government_amount_outstanding")
                
            val crossBondDf=corporateDf.crossJoin(governmentDf)
            val absDiffTermsDf=crossBondDf.withColumn("absDiffTerms", abs(col("corporate_tenor")-col("government_tenor")))
            val rankOnAbsDiffTermsDf=absDiffTermsDf.withColumn("rank",  rank.over(Window.partitionBy("corporate_bond_id").orderBy(col("absDiffTerms"),col("corporate_amount_outstanding").desc)))
            rankOnAbsDiffTermsDf.filter("rank=1")
            
        }
        
        /**
       	*  calculate spread  (corporate_yield - government_yield)
       	*/
        def calculateSpreadDs(df: DataFrame): Dataset[String] = {
           val spreadDf=df.withColumn("spread_to_benchmark", concat(round(col("corporate_yield")-col("government_yield"),2) * 100,lit(" bps")))
           spreadDf.select(collect_list(struct("corporate_bond_id","government_bond_id","spread_to_benchmark")).alias("data")).toJSON
        }
        
}