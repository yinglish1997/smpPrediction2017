package dataDeal;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;

import scala.Tuple2;

public class CData {

	/**
	 * @param args
	 */
	SparkConf conf;
	JavaSparkContext sc;
	static Map<String, String> idValueMap; //UserID, groupingValue
	static Map<String, String> idLabelMap; //UserID, label
	static ArrayList<String> trainID; //拥有成长值的训练集用户id列表
	static ArrayList<String> valiDataID;
	static int n;
	
	public CData(){
		this.conf = new SparkConf().setAppName("createData2").setMaster("local");
		this.sc = new JavaSparkContext(this.conf);
		this.trainID= createTrainedIdList("originalData/trainingData.txt");
		this.idValueMap = userMap("originalData/trainingData.txt", true);
		this.idLabelMap = userMap("originalData/trainingData.txt", false);
		this.valiDataID = createValiDataIdList("testData/task3.txt");
		
	   JavaRDD<String> input = this.sc.textFile("/home/yingying/桌面/smp/result2");
		JavaPairRDD<String, String> idAttsPair = input.mapToPair(new createIdAttPair());
	
	JavaPairRDD<String, String> trainPair = idAttsPair.filter(new choiceTrainUser()); //用这个方法挑选特定的用户
		writeFile("validation/testUserProperties.txt", trainPair, true);
		
//		writeFile("validation/allUserProperties.txt", idAttsPair, true);
////		JavaRDD<LabeledPoint> lp = trainPair.map(new pairToLabeled());
////		List<LabeledPoint>twoLP = lp.take(2);
////		for(LabeledPoint a: twoLP)
////			System.out.println(a.label() + "-> " + a.features());
	
//		//writeFile("classifiData/labelAttributes.txt", trainPair, false);
//		System.out.println(n);
	}
   public static ArrayList<String> createValiDataIdList(String path){
	   File file = new File(path);
	   ArrayList<String> idList = new ArrayList<String>();
	   try{
		   BufferedReader br = new BufferedReader(new FileReader(file));
		   String tmp = "";
		   while((tmp = br.readLine()) != null){
			   idList.add(tmp);
			  // System.out.println(tmp);
		   }
		   br.close();
	   }catch(IOException e){
		   e.printStackTrace();
	   }
//	   for(String s: idList){
//		   if(s.length() != 8)
//			   System.out.println(s);
//	   }
	   System.out.println("UserNumer: : " + idList.size());
	   return idList;
   }
	public static class pairToLabeled implements Function<Tuple2<String, String>, LabeledPoint>{
		public LabeledPoint call(Tuple2<String, String> tup){
			double label = Double.parseDouble(tup._1());
			String[] arr = tup._2().split(" ");
			double[] nums = new double[arr.length];
			for(int i = 0; i < arr.length; i ++)
				nums[i] = Double.parseDouble(arr[i]);
			Vector features = Vectors.dense(nums);
			return new LabeledPoint(label, features);
		}
	}
	public void writeFile(String path, JavaPairRDD<String, String> trainPair, Boolean isValue){
		File file = new File(path);
		//List<Tuple2<String, String>> tupList = trainPair.collect();
		Map<String, String> userMap = trainPair.collectAsMap();
		try{
			BufferedWriter bw = new BufferedWriter(new FileWriter(file, true));
			for(String id: valiDataID){
				String[] arr = userMap.get(id).split(" ");
				//System.out.println(userMap.get(id));
				String line = "";
				for(int i = 0; i < arr.length; i ++){
					line += (i + 1) + ":" + arr[i] + " ";
				}
				bw.write("0 " + line);
				bw.write("\n");
//			for(Tuple2<String, String> tup: tupList){
//					String[] arr = tup._2().split(" ");
//					String line = "";
//			   for(int i = 0; i < arr.length; i ++)
//				   line += (i + 1) + ":" + arr[i] + " ";
//				bw.write("0 " + line);
//				bw.write("\n");			  						
			   
//				if(isValue){
//					String value = idValueMap.get(tup._1());
//					bw.write(value + " " + line);
//					bw.write("\n");
//				}else{
//					String label = idLabelMap.get(tup._1());
//					bw.write(label + " " + line);
//					bw.write("\n");
//				}
			}
			bw.close();
			System.out.println("write File is ok");
		}catch(IOException e){
			e.printStackTrace();
		}
	}
	public static class choiceTrainUser implements Function<Tuple2<String, String>, Boolean>{
		public Boolean call(Tuple2<String, String> tup){
			if(valiDataID.contains(tup._1()))
			//if(trainID.contains(tup._1()))
				return true;
			else return false;
		}
	}
	public ArrayList<String> createTrainedIdList(String trainedFile){
		File file = new File(trainedFile);
		ArrayList<String> onlyID = new ArrayList<String>();
		try{
			BufferedReader br = new BufferedReader(new FileReader(file));			
			String tmp = "";
			while(( tmp = br.readLine()) != null){
				ArrayList<String> idList = new ArrayList<String>();
				String[] arr = tmp.split("\001");
				onlyID.add(arr[0]);// onlyID只放置训练集用户id		
			}
			br.close();
		}catch(IOException e){
			e.printStackTrace();
		}
		return onlyID;
	}
	public Map<String, String> userMap(String trainedFilePath, boolean isValue){
		File file = new File(trainedFilePath);
		Map<String, String> map = new HashMap<String, String>();
		try{
			BufferedReader br =new BufferedReader(new FileReader(file));
			String tmp = "";
			while( (tmp = br.readLine()) != null){
				String[] arr = tmp.split("\001");
				if(isValue)
					map.put(arr[0], arr[1]); //UserID, value
				else{
					map.put(arr[0], classLabel(arr[1]));  //UserID, label
				}
			}
			br.close();
			System.out.println("map is ok ! " + map.size() );
		}catch(IOException e){
			e.printStackTrace();
		}
		return map;
	}
	public String classLabel(String valueStr){
		double value = Double.parseDouble(valueStr);
		if(value < 0.002)
			return "0.0";
		else if(value <= 0.01)
			return "1.0";
		else return "2.0";
	}
	public static class createIdAttPair implements PairFunction<String, String, String>{
		public Tuple2<String, String> call(String s){
			
			String id = s.split(" ")[0];
			String[] arr = s.replaceAll(id, "").split(" ");
			String atts = "";
			for(int i = 0; i < arr.length; i ++){
				if( !arr[i].equals("") )
					atts += arr[i] + " ";
			}
			int n = atts.split(" ").length;
			if(atts.split(" ").length != 437)
				System.out.println("exits wrong: " + atts.split(" ").length);
			return new Tuple2<String, String>(id, atts);
		}
	}
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		CData test = new CData();
	}

}
