package dataDeal;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import dataDeal.CreateData.blogPair;

import scala.Tuple2;


public class Test {

	/**	
	 * @param args
	 */
	SparkConf conf ;
	JavaSparkContext sc ;
	ArrayList<ArrayList<String>> trainDataList; 
	JavaPairRDD<String, String> userWordsPair; //【训练集用户：发表的博文总字数】
	static ArrayList<ArrayList<String>> trainedUserIDList;
	public Test(){
		this.conf = new SparkConf().setAppName("smpData").setMaster("local");
		this.sc = new JavaSparkContext(conf);		
		
	}
	public JavaPairRDD<String, String> createUserWordsPair(){
		JavaPairRDD<String, String> blogUserPair = createBlogPair("originalData/2_Post.txt", false) ; //blogID: userID
		JavaPairRDD<String, String> blogWordsPair = createBlogPair("originalData/contentLength.txt", true) ; //blogID: words
		JavaPairRDD<String, Iterable<String>> blogPair = blogUserPair.union(blogWordsPair).groupByKey();
		JavaPairRDD<String, String> userWordsPair = blogPair.values().mapToPair(new createUserWordsPair());
		JavaPairRDD<String, String> trainedUserWords = userWordsPair.filter(new pickTrainedUser());	
		return trainedUserWords;
	}
	public static class pickTrainedUser implements Function<Tuple2<String, String>, Boolean>{
		//用以挑选训练集用户
		public Boolean call(Tuple2<String, String> tup){
			if(trainedUserIDList.contains(tup._1().substring(0, 8)))
				return true;
			else return false;
		}
	} 	
	public static class createUserWordsPair implements PairFunction<Iterable<String>, String, String>{
		public Tuple2<String, String> call(Iterable<String> userWordsIter){
			Iterator<String> iter = userWordsIter.iterator();
			String str = iter.next();
		    if(str.contains("U")){
					return new Tuple2<String, String>(str, iter.next());
			}else return new Tuple2<String, String>(iter.next(), str);	
		}
	}
	public JavaPairRDD<String, String> createBlogPair(String path, Boolean isContentLength){
		JavaRDD<String> input = sc.textFile(path);
		JavaPairRDD<String, String> blogUserPair = input.mapToPair(new blogPair(isContentLength));
		return blogUserPair;
	}
	public static class blogPair implements PairFunction<String, String, String>{
		private Boolean isContentLength;
		public blogPair(Boolean bl){
			this.isContentLength = bl;
		}
		public Tuple2<String, String> call(String record){
			if(this.isContentLength){
				String[] arr = record.split("	");
				return new Tuple2<String, String>(arr[0], arr[1]);
			}else{
				String[] splited = record.split("\001");
				return new Tuple2<String, String>(splited[1], splited[0]);
			}		
		}
	}
	public static int max(ArrayList<Integer> list){
		int maxNum = list.get(0);
		for(int a: list){
			if(a > maxNum)
				maxNum = a;
		}
		return maxNum;
	}
	public static void main(String[] args) throws IOException {
		File file = new File("testData/TestSet_Task3.txt");
		ArrayList<String> list = new ArrayList<String>();
		BufferedReader br = new BufferedReader(new FileReader(file));
		try{
			String tmp = "";
			while( (tmp = br.readLine()) != null){
				list.add(tmp);
			}
		}catch(IOException e){
			e.printStackTrace();
		}
		br.close();
		System.out.println(list.size());
		for(String name: list)
			System.out.println(name);
	}

}
