
import static org.apache.spark.sql.functions.col;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.classification.DecisionTreeClassificationModel;
import org.apache.spark.ml.classification.DecisionTreeClassifier;
import org.apache.spark.ml.classification.RandomForestClassificationModel;
import org.apache.spark.ml.classification.RandomForestClassifier;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.IndexToString;
import org.apache.spark.ml.feature.RegexTokenizer;
import org.apache.spark.ml.feature.StopWordsRemover;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class classification {

	public static void main(String[] args) throws AnalysisException {
		
		Logger.getLogger("org").setLevel(Level.ERROR);
		Logger.getLogger("akka").setLevel(Level.ERROR);

        SparkSession sparkSession = SparkSession.builder()  //SparkSession  
                .appName("SparkML") 
                .master("local[*]") 
                .getOrCreate(); //
      
        Dataset<Row> userKnowDf = sparkSession.read()
        		.option("header", true)
        		.option("inferSchema",true)
        		.option("mode", "DROPMALFORMED")
        		.csv("data/gender-classifier-DFE-791531.csv");
        //Reading Data from a CSV file //Inferring Schema and Setting Header as True
        Dataset<Row> reduced = userKnowDf.select(
        		col("_golden"),
        		col("gender"),
        		col("gender:confidence"),
        		col("gender_gold"),
        		col("description"),
        		col("sidebar_color"),
        		col("link_color"),
        		col("text"),
        		col("tweet_location"),
        		col("user_timezone")
        		)
        		.filter("`gender:confidence` == 1") //filter low
        		//gender confidence records for better model accuracy
        		.filter("gender == 'male' or gender == 'female' or gender == 'brand'"); //eliminate
        		//unreliable data
        
        
        reduced.show();
        System.out.println(reduced.count());
        
        
        
        RegexTokenizer tokenizer = new RegexTokenizer()
          .setInputCol("text")
          .setOutputCol("words")
        	.setPattern("\\W");
        

        
        Dataset<Row> tokenized = tokenizer.transform(reduced);
        
        //remove commonly used words
        StopWordsRemover remover = new StopWordsRemover()
        		  .setInputCol("words")
        		  .setOutputCol("filtered_words");
        
        
        Dataset<Row> filtered = remover.transform(tokenized);
        filtered = filtered.alias("parent");
        filtered.printSchema();
        filtered.createTempView("parent");
        

        //split single line text into individual rows for each word using explode udf 
        Dataset<Row> word_result = filtered.sqlContext()
        		.sql("SELECT _golden, gender,`gender:confidence`,gender_gold"
                		+ ",sidebar_color,link_color,tweet_location,user_timezone, word "
                		+ "FROM parent LATERAL VIEW explode(filtered_words) childTable AS word");
                		
        word_result.show();
        
        StringIndexer gender_indexer = new StringIndexer()
        		.setInputCol("gender").setOutputCol("gender_ind");
        StringIndexer sbc_indexer = new StringIndexer()
        		.setInputCol("sidebar_color").setOutputCol("sbcolor_ind");
        StringIndexer link_indexer = new StringIndexer()
        		.setInputCol("link_color").setOutputCol("lnkcolor_ind");
        StringIndexer word_indexer = new StringIndexer()
        		.setInputCol("link_color").setOutputCol("word_ind");
        
        
        StringIndexerModel gender_indexed_model =  gender_indexer.fit(word_result);
        Dataset<Row> word_result_indexed = gender_indexed_model.transform(word_result);        
        
        StringIndexerModel  sbc_indexed_model =  sbc_indexer.fit(word_result_indexed);
        word_result_indexed = sbc_indexed_model.transform(word_result_indexed);
        
        StringIndexerModel link_indexed_model =  link_indexer.fit(word_result_indexed);
        word_result_indexed = link_indexed_model.transform(word_result_indexed);
        
        StringIndexerModel word_indexed_model =  word_indexer.fit(word_result_indexed);
        word_result_indexed = word_indexed_model.transform(word_result_indexed);
        word_result_indexed.show();
        word_result_indexed = word_result_indexed.select(
        		col("gender_ind").as("label"),
        		col("word_ind"),
        		col("sbcolor_ind"),
        		col("lnkcolor_ind")
        		);
        		
        
        VectorAssembler assembler = new VectorAssembler()
                .setInputCols(new String[]{"word_ind","sbcolor_ind","lnkcolor_ind"})
                .setOutputCol("features");
        
        word_result_indexed = assembler.transform(word_result_indexed).select("label","features");
        
        Dataset<Row>[] splits = word_result_indexed.randomSplit(new double[]{0.7, 0.3},46L);
        Dataset<Row> trainingData = splits[0];		//Training Data
        Dataset<Row> testData = splits[1];			//Testing Data
        
        
        // Convert indexed labels back to original labels.
   		IndexToString labelConverter = new IndexToString().setInputCol("label").setOutputCol("labelStr")
   				.setLabels(gender_indexed_model.labels());

   		IndexToString predConverter = new IndexToString().setInputCol("prediction").setOutputCol("predictionStr")
   				.setLabels(gender_indexed_model.labels());
        
        //************Decision Tree****************//
        
        DecisionTreeClassifier dt = new DecisionTreeClassifier().setLabelCol("label").setFeaturesCol("features").setSeed(0);
        dt.setMaxDepth(20);
        dt.setMinInfoGain(0.0);
        dt.setMinInstancesPerNode(1);
        dt.setCacheNodeIds(false);
        dt.setMaxBins(3000);
        
        DecisionTreeClassificationModel Model = dt.fit(trainingData);	
        System.out.println("Learned Decision tree" + Model.toDebugString());

		Dataset<Row>  rawPredictions = Model.transform(testData);
		Dataset<Row> predictions = predConverter.transform(labelConverter.transform(rawPredictions));
		predictions.select("predictionStr", "labelStr", "features").show(5);
		//***Test data predictions***
		Dataset<Row> trainRawPredictions = Model.transform(trainingData);
		Dataset<Row> trainPredictions = predConverter.transform(labelConverter.transform(trainRawPredictions));
		trainPredictions.select("predictionStr", "labelStr", "features").show(5);
		
		
		//************Random Forest****************//
		
		RandomForestClassifier rf = new RandomForestClassifier();
		rf.setLabelCol("label").setFeaturesCol("features").setSeed(0);
		rf.setMaxDepth(25);
		rf.setMinInfoGain(0.0);
		rf.setMinInstancesPerNode(1);
		rf.setCacheNodeIds(false);
		rf.setMaxBins(3000);
		
		RandomForestClassificationModel rfModel = rf.fit(trainingData);	
		System.out.println("Learned Random Forest Decision tree" + rfModel.toDebugString());
		Dataset<Row> rfRawPredictions = rfModel.transform(testData);
		Dataset<Row> rfPredictions = predConverter.transform(labelConverter.transform(rfRawPredictions));
		rfPredictions.select("predictionStr", "labelStr", "features").show(5);
		rfPredictions.show();
		
		//***Test data predictions**
		Dataset<Row> rfTrainRawPredictions = rfModel.transform(trainingData);
		Dataset<Row> rfTrainPredictions = predConverter.transform(labelConverter.transform(rfTrainRawPredictions));
		rfTrainPredictions.select("predictionStr", "labelStr", "features").show(5);	

		
		
		
		/*************************Model Evaluation*********************/
		// View confusion matrix
		double accuracy,trainAccuracy;
		// Accuracy computation
		MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator().setLabelCol("label")
				.setPredictionCol("prediction").setMetricName("accuracy"); 
		
		
		
		
		System.out.println("*****Decision Tree*********\n");
		accuracy = evaluator.evaluate(predictions);
		trainAccuracy = evaluator.evaluate(trainPredictions);
		System.out.println("Confusion Matrix :");
		predictions.groupBy(col("labelStr"), col("predictionStr")).count().show();
		System.out.println("Accuracy = " + Math.round(accuracy * 100) + " %");
		System.out.println("Training Dataset Accuracy = " + Math.round(trainAccuracy * 100) + " %");
		Dataset<Row> dtPredictionandlabels = rawPredictions.select(col("prediction"),col("label"));
		MulticlassMetrics dtmetrics = new MulticlassMetrics(dtPredictionandlabels);
		Matrix dtconfusion = dtmetrics.confusionMatrix();
		System.out.println("Confusion matrix 2: \n" + dtconfusion);
		System.out.println("Accuracy 2 = " + dtmetrics.accuracy());
		for (int i = 0; i < dtmetrics.labels().length; i++) {
			  System.out.format("Class %f precision = %f\n", dtmetrics.labels()[i],dtmetrics.precision(
					  dtmetrics.labels()[i]));
			  System.out.format("Class %f recall = %f\n", dtmetrics.labels()[i], dtmetrics.recall(
					  dtmetrics.labels()[i]));
			  System.out.format("Class %f F1 score = %f\n", dtmetrics.labels()[i], dtmetrics.fMeasure(
					  dtmetrics.labels()[i]));
			}
		System.out.format("Weighted precision = %f\n", dtmetrics.weightedPrecision());
		System.out.format("Weighted recall = %f\n", dtmetrics.weightedRecall());
		System.out.format("Weighted F1 score = %f\n", dtmetrics.weightedFMeasure());
	

		
		
		
		System.out.println("*****Random Forest Tree*********\n");
		accuracy = evaluator.evaluate(rfPredictions);
		trainAccuracy = evaluator.evaluate(rfTrainPredictions);
		System.out.println("Confusion Matrix :");
		rfPredictions.groupBy(col("labelStr"), col("predictionStr")).count().show();
		System.out.println("Accuracy = " + Math.round(accuracy * 100) + " %");
		System.out.println("Training Dataset Accuracy = " + Math.round(trainAccuracy * 100) + " %");
		Dataset<Row> predictionandlabels = rfRawPredictions.select(col("prediction"),col("label"));
		MulticlassMetrics metrics = new MulticlassMetrics(predictionandlabels);
		Matrix confusion = metrics.confusionMatrix();
		System.out.println("Confusion matrix 2: \n" + confusion);
		System.out.println("Accuracy 2 = " + metrics.accuracy());
		for (int i = 0; i < metrics.labels().length; i++) {
			  System.out.format("Class %f precision = %f\n", metrics.labels()[i],metrics.precision(
			    metrics.labels()[i]));
			  System.out.format("Class %f recall = %f\n", metrics.labels()[i], metrics.recall(
			    metrics.labels()[i]));
			  System.out.format("Class %f F1 score = %f\n", metrics.labels()[i], metrics.fMeasure(
			    metrics.labels()[i]));
			}
		System.out.format("Weighted precision = %f\n", metrics.weightedPrecision());
		System.out.format("Weighted recall = %f\n", metrics.weightedRecall());
		System.out.format("Weighted F1 score = %f\n", metrics.weightedFMeasure());
		
        		

	}

}
