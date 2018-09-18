package similarDocum;

import java.util.ArrayList;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class CollaborateSource {

	/**
	 * @param args
	 */
	
	public static double cosineSimilar(ArrayList<Double> listOne, ArrayList<Double> listTwo){
		double sum = 0;
		for(int i = 0; i < listOne.size(); i ++){
			sum += listOne.get(i) * listTwo.get(i);
		}
		double listOneSum = 0;
		for(double a: listOne){
			listOneSum += Math.pow(a, 2);
		}
		double listTwoSum = 0;
		for(double b: listTwo){
			listTwoSum += Math.pow(b, 2);
		}
		double similar = sum / ( Math.sqrt(listOneSum) * Math.sqrt(listTwoSum));
		return similar;
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Word2vec one = new Word2vec("/home/yingying/桌面/smp/oneblog/虚拟现实.txt");
		Word2vec two = new Word2vec("/home/yingying/桌面/smp/oneblog/虚拟化.txt");
		Word2vec three = new Word2vec("/home/yingying/桌面/smp/oneblog/机器人.txt");
		Word2vec four = new Word2vec("/home/yingying/桌面/smp/oneblog/数据可视化.txt");
		ArrayList<Double> oneList = one.resultList;
		ArrayList<Double> twoList = two.resultList;
		ArrayList<Double> threeList = three.resultList;
		ArrayList<Double> fourList = four.resultList;
		System.out.println("虚拟现实 Vs 虚拟化:  " + cosineSimilar( oneList, twoList));  
		System.out.println("虚拟现实 Vs 机器人:  " + cosineSimilar(oneList, threeList)); 
		System.out.println("虚拟现实 Vs 数据可视化:  " + cosineSimilar(oneList, fourList)); 
	}

}
