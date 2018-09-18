package classificatioin;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.ml.feature.StandardScaler;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.VectorUDT;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.DecisionTree;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;
import org.apache.spark.mllib.util.MLUtils;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;


import scala.Tuple2;

public class DecisionTreeClassification {

	/**决策树分类用户，包括数据的归一化处理
	 * @param args
	 */
	SparkConf sparkConf ;
	JavaSparkContext jsc;
	JavaRDD<LabeledPoint> trainingData;
	JavaRDD<LabeledPoint> testData;
	static DecisionTreeModel model;
	
	public DecisionTreeClassification(String trainDataPath){
		//参数：形如【成长值：年书写总字数，发表活跃月数目，浏览活跃日数目，评论/点赞/点踩/喜欢/关注/私信数目】的数据文件路径
		//首先会做数据的归一化处理，顺便把归一化后的数据写入新的文件，观察用
		this.sparkConf = new SparkConf().setAppName("javaDecisionTreeClassification").setMaster("local");
		this.jsc = new JavaSparkContext(sparkConf);
		JavaRDD<LabeledPoint> data = MLUtils.loadLibSVMFile(jsc.sc(), trainDataPath).toJavaRDD();
		
		//归一化数据
		JavaRDD<LabeledPoint> testset = normalData(data);
/*
		//把数据切分为训练集和测试集
		JavaRDD<LabeledPoint>[] splits = testset.randomSplit(new double[]{0.7, 0.3});
		this. trainingData = splits[0];
		this.testData = splits[1];
		
		//训练模型
		int numClasses = 4;
		Map<Integer, Integer> categoricalFeaturesInfo = new HashMap<>();
		String impurity = "gini";
		int maxDepth = 5;
		int maxBins = 32;
		this.model = DecisionTree.trainClassifier(this.trainingData, numClasses, categoricalFeaturesInfo, impurity, maxDepth, maxBins);
		
		//打印输出准确率
		double testErr = calculateAccuracy();
		System.out.println(testErr);  //0.7019867549668874
		*/
	}
	
	public JavaRDD<LabeledPoint> normalData(JavaRDD<LabeledPoint> data){
		//为了做数据归一化，要把labeledPoint转成row->DataFrame的形式
		//输入参数：原来的还未处理的LabeledPoint。返回归一化后的LP
		JavaRDD<Row> jrow = data.map(new LabeledPointToRow());
		StructType schema = new StructType(new StructField[]{
				new StructField("label", DataTypes.DoubleType, false, Metadata.empty()),
				new StructField("features", new VectorUDT(), false, Metadata.empty())
		});
		SQLContext jsql = new SQLContext(jsc);
		DataFrame df = jsql.createDataFrame(jrow, schema);		
		//Normalize 
		//StandardScaler scaler = StandardScaler.load("target/tmp/StandardScaler");
		StandardScaler scaler = new StandardScaler().setInputCol("features").setOutputCol("scaled").setWithMean(false).setWithStd(true);
		DataFrame scalerDF = scaler.fit(df).transform(df).select("label", "scaled");		
		//把DataFrame形式转回LabeledPoint
		JavaRDD<Row> selectedrows = scalerDF.javaRDD();
		JavaRDD<LabeledPoint> testset = selectedrows.map(new RowToLabel());
	   //归一化的数据写入文件
		List<LabeledPoint> normalNewData = testset.collect();	  
	   createClassifiFile(normalNewData, "originalData/Data2.txt");
//		for(LabeledPoint lp: normalNewData){
//			System.out.println(lp.label());
//		//		for(int i = 0; i < lp.features().size(); i ++)
//				System.out.print(lp.features().apply(i) + "   ");
//			System.out.print("\n");
//		}
	   return testset;
	}
	public double calculateAccuracy(){
		//计算准确率
		JavaPairRDD<Double, Double> predictionAndLabel = testData.mapToPair(new predictAndLabelPair());
//		List<Tuple2<Double, Double>> list = predictionAndLabel.collect();
//		for(Tuple2<Double, Double> tup: list){
//			System.out.println("pre: " + tup._1() + ", label: " + tup._2());
//		}
		double correctPair = predictionAndLabel.filter(new choicePredictEqualLabel()).count();
		double testErr = correctPair / testData.count() ;
		System.out.println("correctPair: " + correctPair); //212.0
		System.out.println("testDataPair: " + testData.count()); //302		
		return testErr;
	}
	
	public void createClassifiFile (List<LabeledPoint> list, String file){
		//把归一化后的新数据写入文件
		//参数：list：新的LabeledPoint，file：存储文件地址
		File storeFile = new File(file);
		DecimalFormat df = new DecimalFormat("#.####");
		try{
			BufferedWriter bw = new BufferedWriter(new FileWriter(storeFile, true));
			for(LabeledPoint lp: list){
				String line = String.valueOf( lp.label() - 1) + " ";
				for(int i = 0; i < lp.features().size(); i ++){
					line += (i + 1) + ":" + df.format( lp.features().apply(i) ) + " ";
					//line += df.format( lp.features().apply(i) ) + " ";
				}
				bw.write(line);
				bw.write("\n");
			}
			bw.close();
			System.out.println("createClassifiFile is ok !");
		}catch(IOException e){
			e.printStackTrace();
		}
	}
	public static class RowToLabel implements Function<Row, LabeledPoint>{
		public LabeledPoint call(Row r){
			Vector features = r.getAs(1);
			double label = r.getDouble(0);
			return new LabeledPoint(label, features);
		}
	}
	public static class LabeledPointToRow implements Function<LabeledPoint, Row>{
		public Row call(LabeledPoint p){
			double label = p.label();
			Vector vector = p.features();
			return RowFactory.create(label, vector);
		}
	}
	public static class choicePredictEqualLabel implements Function<Tuple2<Double, Double>, Boolean>{
		//计算准确率，过滤掉预测标签与真实标签不一样的RDD
		public Boolean call(Tuple2<Double, Double> tup){
			if(tup._1().equals(tup._2())) return true;
			else return false;
		}
	}
	public static class predictAndLabelPair implements PairFunction<LabeledPoint, Double, Double>{
		//返回元组【预测标签，真实标签】
		public Tuple2<Double, Double> call(LabeledPoint lp){
			return new Tuple2<Double, Double> (model.predict(lp.features()), lp.label());
		}
	}
	public static void main(String[] args) {
		DecisionTreeClassification test = new DecisionTreeClassification("originalData/Data.txt");

	}

}
