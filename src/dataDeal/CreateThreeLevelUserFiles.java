package dataDeal;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;



public class CreateThreeLevelUserFiles {

	/**
	 * @param args
	 */
 
    public CreateThreeLevelUserFiles(){
    	SparkConf conf = new SparkConf().setAppName("create").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> input = sc.textFile("classifiData/valueAttributes.txt");
		JavaPairRDD<String, String> valueAttPair = input.mapToPair(new createPair());
		JavaPairRDD<String, String> zeroLevel = valueAttPair.filter(new filterGroup(0));
		JavaPairRDD<String, String> oneLevel = valueAttPair.filter(new filterGroup(1));
		JavaPairRDD<String, String> twoLevel = valueAttPair.filter(new filterGroup(2));
		writeFile("classifiData/zeroLevel.txt", zeroLevel);
		writeFile("classifiData/oneLevel.txt", oneLevel);
		writeFile("classifiData/twoLevel.txt", twoLevel);
    }
    public void writeFile(String where, JavaPairRDD<String, String> pair){
    	File file = new File(where);
    	List<Tuple2<String, String>> tupList = pair.collect();
    	try{
    		BufferedWriter bw = new BufferedWriter(new FileWriter(file, true));
    		for(Tuple2<String, String> tup: tupList){
    			bw.write(tup._1() + " " + tup._2());
    			bw.write("\n");
    		}
    		bw.close();
    		System.out.println(tupList.size() + "  ok !");
    	}catch(IOException e){
    		e.printStackTrace();
    	}
    }
    public static class filterGroup implements Function<Tuple2<String, String>, Boolean>{
    	private int level ;
    	public filterGroup(int n){
    		this.level = n;
    	}
    	public Boolean call(Tuple2<String, String> tup){
    		double value = Double.parseDouble(tup._1());
    		if(this.level == 0){
    			if(value < 0.002)
    				return true;
    			else return false;
    		}else if(this.level == 1){
    			if(value >= 0.002 && value <= 0.01)
    				return true;
    			else return false;
    		}else{
    			if(value > 0.01)
    				return true;
    			else return false;
    		}
    	}
    }
    public static class createPair implements PairFunction<String, String, String>{
    	public Tuple2<String, String> call(String s){
    		return new Tuple2<String, String> (s.split(" ")[0], s.substring(7));
    	}
    }
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		CreateThreeLevelUserFiles test = new CreateThreeLevelUserFiles();
		
	}

}
