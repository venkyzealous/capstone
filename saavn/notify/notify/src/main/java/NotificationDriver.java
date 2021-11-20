import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.clustering.BisectingKMeans;
import org.apache.spark.ml.clustering.BisectingKMeansModel;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;


import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.dense_rank;
import static org.apache.spark.sql.functions.rank;
import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.count;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;


public class NotificationDriver 
{
    public static void main( String[] args ) throws AnalysisException
    {
    	
		Logger.getLogger("org").setLevel(Level.ERROR);
		Logger.getLogger("akka").setLevel(Level.ERROR);

		

		
		
        SparkSession sparkSession = SparkSession.builder()  //SparkSession  
                .appName("SparkML") 
                .master("local[*]")
                .getOrCreate(); //
        
        
        //Load data from s3 buckets
        Dataset<Row> rawData = sparkSession.read().format("csv").option("inferschema", "false")
        		.load("s3a://bigdataanalyticsupgrad/activity/sample100mb.csv").toDF("userid","timestamp","songid","date_orig");
        
        Dataset<Row> rawMetaData = sparkSession.read().format("csv").option("inferschema", "false")
        		.load("s3a://bigdataanalyticsupgrad/newmetadata").toDF("songid","artistid");
        
        Dataset<Row> notification_clicks = sparkSession.read().format("csv").option("inferschema", "false")
        		.load("s3a://bigdataanalyticsupgrad/notification_clicks").toDF("notificationid","userid","date_orig");
        
        Dataset<Row> notification_artists = sparkSession.read().format("csv").option("inferschema", "false")
        		.load("s3a://bigdataanalyticsupgrad/notification_actor").toDF("notificationid","artistid");
        
        //Format date column and remove timestamp column
        rawData = rawData.withColumn("date", 
        		functions.unix_timestamp(rawData.col("date_orig"), "yyyyMMdd").cast("string"))
        		.drop("date_orig");
        		
   		notification_clicks = notification_clicks.withColumn("date", 
   				functions.unix_timestamp(notification_clicks.col("date_orig"), "yyyyMMdd").cast("string"))
   				.drop("date_orig");
        
        rawData.drop(col("timestamp"));
        
        //Combine clickstream and metadata to identify artists
        rawData = rawData.join(rawMetaData, "songid");
        
        rawData.printSchema();

        
        StringIndexer userid_indexer = new StringIndexer()
        		.setInputCol("userid").setOutputCol("userid_ind");
        
       
        StringIndexer date_indexer = new StringIndexer()
        		.setInputCol("date").setOutputCol("date_ind");
        
        StringIndexer artist_indexer = new StringIndexer()
        		.setInputCol("artistid").setOutputCol("artistid_ind");

        
        VectorAssembler assembler = new VectorAssembler()
                .setInputCols(new String[]{"userid_ind","artistid_ind","date_ind"}) 
                .setOutputCol("features");
        
       
        //Prepare data to process in pipeline mode
        Pipeline pipeline = new Pipeline()
				.setStages(new PipelineStage[] {userid_indexer,artist_indexer,date_indexer, assembler});
		
        PipelineModel pmodel = pipeline.fit(rawData);		
        Dataset<Row> assembled = pmodel.transform(rawData);
        
        //Measure WSSSE to get initial K Value
		if (args[0].equals("-findk")) {
			// FindingK
			List<Integer> numClusters = Arrays.asList(2, 4, 6, 8, 10, 12, 14, 16);
			for (Integer k : numClusters) {
				KMeans kmeans = new KMeans().setK(k).setSeed(1L);
				KMeansModel model = kmeans.fit(assembled);

				System.out.println("==========================");
				System.out.println("k = " + k);
				
				// Within Set Sum of Square Error (WSSSE).
				double WSSSE = model.computeCost(assembled);
				System.out.println("WSSSE = " + WSSSE);

				// Shows the results
				Vector[] centers = model.clusterCenters();
				System.out.println("Cluster Centers: ");
				for (Vector center : centers) {
					System.out.println(center);
				}
				System.out.println("==========================");
			}
		}

		else {
		//Use passed K value to perform classification	
		int k = Integer.parseInt(args[1]);
        //Generate Model & Predict
        BisectingKMeans bkm = new BisectingKMeans().setK(k).setSeed(1);
        BisectingKMeansModel model = bkm.fit(assembled);
        Dataset<Row> predictions = model.transform(assembled);
        
          
   		//Identify most popular artist of a each cohort obtained by prediction
        Dataset<Row> cohorts = predictions.groupBy(col("prediction"),col("artistid")).count()
        		.orderBy(col("prediction"), col("count").desc());
        
        WindowSpec spec = Window.partitionBy("prediction").orderBy(col("count").desc());
        
        Dataset<Row> popularartists = cohorts.withColumn("rank", dense_rank().over(spec)).select("prediction",
        		"artistid",
        		"count").where("rank == 1");
        
        popularartists = popularartists.withColumnRenamed("artistid","topartistid");
        predictions = predictions.withColumnRenamed("userid", "tonotifyuserid");

        //Combine notification artist and actual click information 
   		Dataset<Row> notifications = notification_clicks.join(notification_artists,"notificationid");
   		
   		notifications = notifications.withColumnRenamed("userid", "clickeduserid");
   		
   		
        //Identify the list of users to be notified using all cohorts with most popular artists as the one to be
   		//notified
        Dataset<Row> notifylist = predictions.select(col("tonotifyuserid"),col("prediction"))
        		.join(popularartists,predictions.col("prediction").equalTo(popularartists.col("prediction")),"LeftOuter")
        		.join(notifications,popularartists.col("topartistid").equalTo(notifications.col("artistid"))
        				.and(predictions.col("tonotifyuserid").equalTo(notifications.col("clickeduserid")))
        				,"LeftOuter");
        				
        //Measure click through rate
        Dataset<Row> ctrresults = notifylist.groupBy(col("notificationid"),col("topartistid"))
        		.agg(count("tonotifyuserid").as("notifiedcount") , count("clickeduserid").as("clickedcount"));
        
        ctrresults = ctrresults.withColumn("ctr", expr("clickedcount/notifiedcount"))
        		.orderBy(col("ctr").desc());
        
      
        //Save top notifications with max CTR
        Dataset<Row> topNotifications = ctrresults.select(col("notificationid"),col("ctr")).limit(5);
        topNotifications
        .repartition(1)
        .write()
        .format("csv")
        .option("header", "true")
        .mode("overwrite")
        .save(".\\CTR");
        
        
        //Save Notified Users & Artists for each top 5 notifications 
        Iterator<Row> iterator = topNotifications.toLocalIterator(); 
        while(iterator.hasNext()) {
        	Row row = iterator.next();
        	String notificationid = row.getAs("notificationid");
        	
        	Dataset<Row> clusterrows = notifylist.select(col("notificationid"),
        			col("tonotifyuserid").as("userid"),
        			col("topartistid").as("artistid"))
        			.where("notificationid="+notificationid+
        					" AND topartistid is not null "+
        					"and tonotifyuserid is not null");
        	clusterrows
        	.repartition(1)
        	.write()
        	.option("header", "true")
            .format("csv")
            .mode("overwrite")
            .save(".\\notificationclusters\\"+notificationid);
        }
        
        WindowSpec artistwindowspec = Window.orderBy(col("topartistid"));
        
        //Save all clusters (combination of users across all cohorts with given artist as most popular one for those
        //cohorts
        Dataset<Row> clusters = notifylist.select(col("tonotifyuserid"),col("topartistid"))
        .where("topartistid is not null").withColumn("clusterid", rank().over(artistwindowspec));
        
        clusters
        .repartition(1)
    	.write()
    	.option("header", "true")
        .format("csv")
        .mode("overwrite")
        .save(".\\UserClusterArtist");
        
   		
        
       
		}
        
    }
}
