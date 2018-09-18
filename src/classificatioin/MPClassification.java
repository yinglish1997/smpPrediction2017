package classificatioin;


import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.ml.classification.MultilayerPerceptronClassificationModel;
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.ChiSqSelector;
import org.apache.spark.ml.feature.ChiSqSelectorModel;
import org.apache.spark.ml.feature.StandardScaler;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.VectorUDT;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;


public class MPClassification {

	/**多层感知机用户分类
	 * @param args
	 */
	SparkConf conf;
	SparkContext sc;
	static SQLContext sqlContext;
	static int selectedFeatureNums ;
	public MPClassification(String libSvmPath){
		this.conf = new SparkConf().setMaster("local").setAppName("MPClassification");
		this.sc = new SparkContext(this.conf);
		this.sqlContext = new SQLContext(this.sc);
		this.selectedFeatureNums = 75;
		
		DataFrame dataFrame = sqlContext.read().format("libsvm").load(libSvmPath);
		
		DataFrame chiSqSelectedDF = dataFeatureDeal(dataFrame);
			
		MPClassificationCalculate(chiSqSelectedDF);

	}
	public DataFrame dataFeatureDeal(DataFrame dataFrame){
		//数据归一化处理
		StandardScaler scaler = new StandardScaler().setInputCol("features").setOutputCol("scaled").setWithMean(false).setWithStd(true);
		DataFrame scalerDF = scaler.fit(dataFrame).transform(dataFrame).select("label", "scaled");
		
		JavaRDD<Row> selectedrows = scalerDF.javaRDD();
		
		StructType schema = new StructType(new StructField[]{
				new StructField("label", DataTypes.DoubleType, false, Metadata.empty()),
				new StructField("features", new VectorUDT(), false, Metadata.empty())
		});
		SQLContext jsql = new SQLContext(sc);
		DataFrame df = jsql.createDataFrame(selectedrows, schema);
		df.show();
		
		//特征选择
		ChiSqSelector selector = new ChiSqSelector().setNumTopFeatures(this.selectedFeatureNums).setFeaturesCol("features")
				.setLabelCol("label").setOutputCol("selectedFeatures");
		ChiSqSelectorModel chiModel = selector.fit(df);
		
		DataFrame chiSqSelectedDF = chiModel.transform(df).select("label", "selectedFeatures").withColumnRenamed("selectedFeatures", "features");
		chiSqSelectedDF.show();
		
		return chiSqSelectedDF;
	}
	public void MPClassificationCalculate(DataFrame selectedDF){
		//多层感知器分类
		DataFrame[] splits = selectedDF.randomSplit(new double[] {0.6, 0.4}, 1234L);
		DataFrame train = splits[0];
		DataFrame test = splits[1];
			
		int[] layers = new int[]{this.selectedFeatureNums, 5, 4, 3};
		MultilayerPerceptronClassifier trainer = new MultilayerPerceptronClassifier()
		.setLayers(layers)
		.setBlockSize(128).setSeed(1234L)
		.setMaxIter(100);
		
		MultilayerPerceptronClassificationModel model = trainer.fit(train);
		
		DataFrame result = model.transform(test);
		DataFrame predictionAndLabels = result.select("prediction", "label");
			
		MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator();
		
		System.out.println(selectedDF.columns().length);
		System.out.println("Test set accuracy = " + evaluator.evaluate(predictionAndLabels));	
	}
	
	public DataFrame chiSqSelectData(DataFrame dataFrame){
		//卡方检测挑选属性。输入【标签 下标：属性值……】构成的DataFrame，返回新的属性DataFrame
		ChiSqSelector selector = new ChiSqSelector().setNumTopFeatures(60).setFeaturesCol("features")
				.setLabelCol("label").setOutputCol("selectedFeatures");
		ChiSqSelectorModel chiModel = selector.fit(dataFrame);
		
		DataFrame selectedDF = chiModel.transform(dataFrame).select("label", "selectedFeatures").withColumnRenamed("selectedFeatures", "features");
			return selectedDF;
	}
	public JavaRDD<LabeledPoint> DataFrameToLabeledRDD(DataFrame selectedDF){
		//把DataFrame转换成JavaRDD<LabeledPoint>可以做其他模型运算	
		JavaRDD<LabeledPoint> lp =  selectedDF.javaRDD().map(new rowToLabeledPoint());
		return lp;
	}
	public static class rowToLabeledPoint implements Function<Row, LabeledPoint>{
		//把DataFrame转换成JavaRDD<LabeledPoint>
		public LabeledPoint call(Row r){
			double label = r.getDouble(0);
			Vector features = r.getAs(1);
			return new LabeledPoint(label, features);
		}
	}
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		MPClassification test = new MPClassification("classifiData/valueAttributes.txt");
	}

}
