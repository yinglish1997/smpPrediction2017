package dataDeal;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.ml.feature.StandardScaler;
import org.apache.spark.mllib.linalg.VectorUDT;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import classificatioin.DecisionTreeClassification.LabeledPointToRow;
import classificatioin.DecisionTreeClassification.RowToLabel;

import scala.Tuple2;

public class CreateClassificationData {

	/**
	 * @param args
	 */
	SparkConf conf;
	JavaSparkContext sc;
	
	public CreateClassificationData(String path){
		this.conf = new SparkConf().setAppName("createClassificationData").setMaster("local");
		this.sc = new JavaSparkContext(conf);
		JavaRDD<String> input = this.sc.textFile(path);
		/*
		JavaPairRDD<String, String> valueFeaturePair = input.mapToPair(new createValueFeaturePair());
		JavaPairRDD<String, String> g1Pair = valueFeaturePair.filter(new groupUser(0));
		JavaPairRDD<String, String> g2Pair = valueFeaturePair.filter(new groupUser(1));
		JavaPairRDD<String, String> g3Pair = valueFeaturePair.filter(new groupUser(2));
		writeIntoFile(g1Pair, 0, "classifiData/1.txt");
		writeIntoFile(g2Pair, 1, "classifiData/1.txt");
		writeIntoFile(g3Pair, 2, "classifiData/1.txt");*/
		//1.txt文件是【标签 93个下标：值】的文件，属性值未做归一化
		//JavaRDD<String> input2 = this.sc.textFile("classifiData/1.txt");
		JavaRDD<LabeledPoint> data = MLUtils.loadLibSVMFile(sc.sc(), "classifiData/1.txt").toJavaRDD();
		JavaRDD<LabeledPoint> testset = normalData(data, "classifiData/2.txt");
	}
	public JavaRDD<LabeledPoint> normalData(JavaRDD<LabeledPoint> data, String path){
		//为了做数据归一化，要把labeledPoint转成row->DataFrame的形式
		//输入参数：原来的还未处理的LabeledPoint。返回归一化后的LP
		JavaRDD<Row> jrow = data.map(new LabeledPointToRow());
		StructType schema = new StructType(new StructField[]{
				new StructField("label", DataTypes.DoubleType, false, Metadata.empty()),
				new StructField("features", new VectorUDT(), false, Metadata.empty())
		});
		SQLContext jsql = new SQLContext(sc);
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
	   createClassifiFile(normalNewData, path);
	   return testset;
	}
	public void createClassifiFile (List<LabeledPoint> list, String file){
		//把归一化后的新数据写入文件
		//参数：list：新的LabeledPoint，file：存储文件地址
		File storeFile = new File(file);
		DecimalFormat df = new DecimalFormat("#.####");
		try{
			BufferedWriter bw = new BufferedWriter(new FileWriter(storeFile, true));
			for(LabeledPoint lp: list){
				String line = String.valueOf( lp.label() ) + " ";
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
	public void writeIntoFile(JavaPairRDD<String, String> pair, int group, String path) {
		List<Tuple2<String, String>> tupList = pair.collect();
		File file = new File(path);	
		try{
			BufferedWriter bw = new BufferedWriter(new FileWriter(file, true));
			for(Tuple2<String, String> tup: tupList){
				String[] line = tup._2().split(" ");
				//String record = tup._1() + " ";   tup._1()是成长值
				String record = group + " "; //group是标签
				for(int i = 1; i <= line.length; i ++){
					record +=  line[i - 1] + " ";
				}
				bw.write(record);
				bw.write("\n");
			}
			bw.close();
			System.out.println("group " + group + " has been written into file, " + tupList.size() + " user");
		}catch(IOException e){
			e.printStackTrace();
		}
	}
//	List<LabeledPoint> normalNewData = testset.collect();
//	for(LabeledPoint lp: normalNewData){
//		System.out.println(lp.label());
//		for(int i = 0; i < lp.features().size(); i ++)
//			System.out.print(lp.features().apply(i) + "   ");
//		System.out.print("\n");
//	}
	public static class groupUser implements Function<Tuple2<String, String>, Boolean>{
		private int group;
		public groupUser(int n){
			this.group = n;
		}
		public Boolean call(Tuple2<String, String> tup){
			double value = Double.parseDouble(tup._1());
			if(this.group == 0){
				if(value < 0.002) return true;
				else return false;
			}else if(this.group == 1){
				if(value > 0.002 && value < 0.01) return true;
				else return false;
			}else{
				if(value >= 0.01) return true;
				else return false;
			}
		}
	}
	public static class createValueFeaturePair implements PairFunction<String, String, String>{
		public Tuple2<String, String> call(String s){
			return new Tuple2<String, String> (s.split(" ")[0], s.substring(7));
		}
	}
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		CreateClassificationData test = new CreateClassificationData("originalData/Data.txt");
	}

}
