package prediction;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.ml.feature.ChiSqSelector;
import org.apache.spark.ml.feature.ChiSqSelectorModel;
import org.apache.spark.ml.feature.StandardScaler;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.VectorUDT;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.regression.LinearRegressionModel;
import org.apache.spark.mllib.regression.LinearRegressionWithSGD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;


import org.apache.spark.api.java.function.Function2;

import classificatioin.MPClassification.rowToLabeledPoint;
import scala.Tuple2;
import scala.annotation.serializable;

public class LinearRegression {

	/**
	 * @param args
	 */
	SparkConf conf;
	JavaSparkContext sc;
	private int selectedFeatureNums;
	static SQLContext sqlContext;
	static LinearRegressionModel model;
	
	public  LinearRegression(String path){
		this.conf = new SparkConf().setAppName("LinearRegression").setMaster("local");
		this.sc = new JavaSparkContext(conf);
		this.sqlContext = new SQLContext(this.sc);
		this.selectedFeatureNums =25;
		
		DataFrame dataFrame = sqlContext.read().format("libsvm").load(path);
		//System.out.println(dataFrame.count());
		DataFrame chiSqSelectedDF = dataFeatureDeal(dataFrame);
		JavaRDD<LabeledPoint> linearLP = DataFrameToLabeledRDD(chiSqSelectedDF);
//		List<LabeledPoint> lpList = linearLP.collect();
//		for(LabeledPoint lp: lpList){
//			System.out.println(lp.label() + " -> " );
//			for(int i = 0; i < lp.features().size(); i ++)
//				System.out.print(i + ": " + lp.features().apply(i) + "   ");
//			System.out.print("\n");
//		}
		linearRegressionCalculate(linearLP);
		sc.stop();
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
		return df;
//		//特征选择
//		ChiSqSelector selector = new ChiSqSelector().setNumTopFeatures(this.selectedFeatureNums).setFeaturesCol("features")
//				.setLabelCol("label").setOutputCol("selectedFeatures");
//		ChiSqSelectorModel chiModel = selector.fit(df);
//		
//		DataFrame chiSqSelectedDF = chiModel.transform(df).select("label", "selectedFeatures").withColumnRenamed("selectedFeatures", "features");
//		chiSqSelectedDF.show();
//		
//		return chiSqSelectedDF;
	}
	public void linearRegressionCalculate(JavaRDD<LabeledPoint> parsedData) {
		//训练线性回归模型并计算正确率
		JavaRDD<LabeledPoint>[] splits = parsedData.randomSplit(new double[]{0.6, 0.4});
		JavaRDD<LabeledPoint> trainingData = splits[0];  //748
		JavaRDD<LabeledPoint> testData = splits[1];   //267
		System.out.println("trainingDataNums: " + trainingData.count() + ", testDataNums: " + testData.count());
		//building the model
		int numIterations = 200; //迭代次数
		double stepSize = 0.1; //步长
	    this.model = LinearRegressionWithSGD.train(JavaRDD.toRDD(trainingData), numIterations, stepSize);	
		JavaPairRDD<Double, Double> predsAndValue = testData.mapToPair(new predsAndLabelPair());
		JavaPairRDD<Double, Double> correctPair = predsAndValue.filter(new filterCorrectPair());
		System.out.println("correctPair nums: " + correctPair.count() + ",   allPairNum: " + predsAndValue.count());
		List<Tuple2<Double, Double>> tupList = predsAndValue.collect();
		DecimalFormat df = new DecimalFormat("#.####");
		
		for(Tuple2<Double, Double> tup: tupList)
			System.out.println(df.format( tup._1() ) + "    " +df.format( tup._2() ));
		
//		//测试迭代次数与步长
//		int[] numArr = new int[]{5, 10, 25, 50, 100, 150, 200};
//		ArrayList<Double> numList = new ArrayList<Double>();
//		double[] stepArr = new double[]{ 0.1, 0.15,  0.5, 0.7, 0.8, 0.01, 0.02};
//		ArrayList<Double> stepList = new ArrayList<Double>();
//
//		for(int numIterations: numArr){
//			this.model = LinearRegressionWithSGD.train(JavaRDD.toRDD(trainingData), numIterations, 0.1);			
//			//evaluate model on training examples and compute training error
//			JavaPairRDD<Double, Double> predsAndValue = testData.mapToPair(new predsAndLabelPair());
//			JavaPairRDD<Double, Double> correctPair = predsAndValue.filter(new filterCorrectPair());
//			numList.add( correctPair.count() * 1.0 / predsAndValue.count());
//		}
//		
//		for(double step: stepArr){
//			this.model = LinearRegressionWithSGD.train(JavaRDD.toRDD(trainingData), 100, step);			
//			//evaluate model on training examples and compute training error
//			JavaPairRDD<Double, Double> predsAndValue = testData.mapToPair(new predsAndLabelPair());
//			JavaPairRDD<Double, Double> correctPair = predsAndValue.filter(new filterCorrectPair());
//			stepList.add( correctPair.count() * 1.0 / predsAndValue.count() );
//		}
//		
//		for(int i = 0; i < numArr.length; i ++)
//			System.out.print(numArr[i] + " -> " + numList.get(i) + ",   ");
//		System.out.print("\n");
//		for(int i = 0; i < stepArr.length; i ++)
//			System.out.print(stepArr[i] + " -> " + stepList.get(i) + ",    ");
//		System.out.print("\n");
		
		//打印正确的键值对的预测结果与真实结果，比对
//		DecimalFormat df = new DecimalFormat("#.####");
//		this.model = LinearRegressionWithSGD.train(JavaRDD.toRDD(trainingData), 10, 0.1);
//		JavaPairRDD<Double, Double> predsAndValue = testData.mapToPair(new predsAndLabelPair());
//		JavaPairRDD<Double, Double> correctPair = predsAndValue.filter(new filterCorrectPair());
//		List<Tuple2<Double, Double>> correctList = correctPair.collect();
//		for(Tuple2<Double, Double> tup: correctList){
//			System.out.println(df.format( tup._1()  )+ " vs " + df.format( tup._2()  ));
//		}

	}
    public static class filterCorrectPair implements Function<Tuple2<Double, Double>, Boolean>{
    	public Boolean call(Tuple2<Double, Double> tup){
    		DecimalFormat df = new DecimalFormat("#.####");
    		double pre = Double.parseDouble( df.format(tup._1()) );
    		if( pre >= ( tup._2() - 0.0005) && pre <= (tup._2() + 0.0005) )
    			return true;
    		else return false;
    	}
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

	public static class sumCalculate implements Function2<Double, Double, Double>{
		public Double call(Double a, Double b){
			return a + b;
		}
	}
	public static class subCalculate implements Function<Tuple2<Double, Double>, Double>{
		public Double call(Tuple2<Double, Double> pl) {
			Double diff = pl._1() - pl._2();
			return diff * diff;
		}
	}
	public static class predsAndLabelPair  implements PairFunction<LabeledPoint, Double, Double>{
		//输入是测试数据Labeledpoint，返回的是【预测成长值，真实成长值】元组
		public Tuple2<Double, Double> call(LabeledPoint point){
			double predictNum = model.predict(point.features());
			return new Tuple2<Double, Double>(predictNum, point.label());
		}
	}
	public static class createLP implements Function<String, LabeledPoint>{
		public LabeledPoint call(String line){
			//System.out.println(line);
			String[] parts = line.split(", ");
			String[] features = parts[1].split(" ");
			double[] v = new double[features.length];
			for(int i = 0; i < features.length; i ++){
				//System.out.print(features[i] + "  ");
				v[i] = Double.parseDouble(features[i]);
			}
			//System.out.print("\n");
			return new LabeledPoint(Double.parseDouble(parts[0]), Vectors.dense(v));
		}
	}
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		LinearRegression test = new LinearRegression("classifiData/oneLevel.txt");
	}

}
