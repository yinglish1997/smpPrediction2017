package featureSelect;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.feature.ChiSqSelector;
import org.apache.spark.ml.feature.ChiSqSelectorModel;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

public class ChiSqSelectorFeature {

	/**
	 * @param args
	 */
	public static void writeSelectedFeature (List<Row> rowList){
		File file = new File("");
		try{
			BufferedWriter bw =new BufferedWriter(new FileWriter(file, true));
			for(Row row: rowList){
				for(int i = 0; i < row.size(); i ++)
					System.out.println(row.apply(i));
			}
		}catch(IOException e){
			e.printStackTrace();
		}
	}
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("chiSqSelector").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);
		
		DataFrame dataFrame = sqlContext.read().format("libsvm").load("originalData/classifiData2.txt");
		
		ChiSqSelector selector = new ChiSqSelector().setNumTopFeatures(50).setFeaturesCol("features")
				.setLabelCol("label").setOutputCol("selectedFeatures");
		ChiSqSelectorModel chiModel = selector.fit(dataFrame);
		
		DataFrame selectedDF = chiModel.transform(dataFrame).select("label", "selectedFeatures");


		//selectedDF.show();
	}

}
