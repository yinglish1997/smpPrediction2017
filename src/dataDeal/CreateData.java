package dataDeal;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.DoubleFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class CreateData {

	/**
	 * @param args
	 */
	SparkConf conf ;
	JavaSparkContext sc ;
	JavaPairRDD<String, String> userWordsPair; //【训练集用户：发表的博文总字数】
	
	//JavaPairRDD<String, Integer> userPostPair; //【训练集用户：发表的博文总数】
	//JavaPairRDD<String, Integer> userBrowsePair; //【训练集用户：浏览总数】
	JavaPairRDD<String, Integer> userComPair; //【：评论总数】
	JavaPairRDD<String, Integer> userVoteUpPair; // 【：点赞总数】
	JavaPairRDD<String, Integer> userVoteDownPair;  //【：点踩数】
	JavaPairRDD<String, Integer> userFavoritePair;  //【：喜欢（收藏）总数】
	JavaPairRDD<String, Integer> userFollowPair; //【：关注总数】
	JavaPairRDD<String, Integer> userLetterPair;  //【：私信总数】
	
	JavaPairRDD<String, Integer> userActPostMonPair; // 【：活跃月数】（达到一定数目的日子的总数）
	JavaPairRDD<String, Integer> userActBrowDailyPair; //【：活跃日浏览量】
	
	JavaPairRDD<String, String> userTwelvePostPair ;
	JavaPairRDD<String, String> userTwelveBrowsePair;
	JavaPairRDD<String, String> userTwelveComPair ;
	JavaPairRDD<String, String> userTwelveVoteUpPair;
	JavaPairRDD<String, String> userTwelveVoteDownPair ;
	JavaPairRDD<String, String> userTwelveFavoritePair;
	JavaPairRDD<String, String> userTwelveLetterPair;
	
	JavaPairRDD<String, String> userEdPair; //用户的被动信息数据，
	
	//训练用户数据链表：每条链表元素是一个训练用户id及其成长值。最后一个链表元素放的是trainedUserIDList，即所有训练用户的id。
	ArrayList<ArrayList<String>> trainDataList; 
	
	//存放训练用户的id链表，其实是trainDataList的最后一个元素
	static ArrayList<String> trainedUserIDList ;	
	
	//存放训练用户所有信息的链表
	ArrayList<ArrayList<String>> userInforList;  

	public CreateData(){
		this.conf = new SparkConf().setAppName("smpData").setMaster("local");
		this.sc = new JavaSparkContext(conf);				
		/*
		this.trainDataList = trainDataList("originalData/trainingData.txt");
		this.trainedUserIDList = this.trainDataList.get(this.trainDataList.size() - 1);
		//System.out.println(this.trainDataList.size() + "   " + this.trainedUserIDList.size());
		
		this.userWordsPair = createUserWordsPair();
		//System.out.println(this.userWordsPair.count());	//795	
		
		//用用户的活跃
		this.userActPostMonPair = userIDActiveDayPair("originalData/2_Post.txt", 7 , 4);
		this.userActBrowDailyPair = userIDActiveDayPair("originalData/3_Browse.txt", 8, 4);
		
		this.userComPair = calculateTotalNum("originalData/4_Comment.txt", 0);
		this.userVoteUpPair = calculateTotalNum("originalData/5_Vote-up.txt", 0);		
		this.userVoteDownPair = calculateTotalNum("originalData/6_Vote-down.txt", 0);
		this.userFavoritePair = calculateTotalNum("originalData/7_Favorite.txt", 0);
		this.userFollowPair = calculateTotalNum("originalData/8_Follow.txt", 1);
		this.userLetterPair = calculateTotalNum("originalData/9_Letter.txt", 1);	
//		System.out.println(userPostPair.count());       //121213
//		System.out.println(userVoteUpPair.count()); //23646
		
		this.userInforList = allUserInforList();
		
		this.userTwelvePostPair = createTwelveElementPair("originalData/attributeData/post");
		System.out.println("userTwelvePostPair: " + this.userTwelvePostPair.count());
		
		this.userTwelveBrowsePair = createTwelveElementPair("originalData/attributeData/Browse");
		System.out.println("userTwelvePostPair: " + this.userTwelveBrowsePair.count());
		
		this.userTwelveComPair = createTwelveElementPair("originalData/attributeData/comment");
		System.out.println("userTwelveComPair: " + this.userTwelveComPair.count());
		
		this.userTwelveVoteUpPair = createTwelveElementPair("originalData/attributeData/vote-up");
		System.out.println("userTwelveVoteUpPair: " + this.userTwelveVoteUpPair.count());
		
		this.userTwelveVoteDownPair = createTwelveElementPair("originalData/attributeData/vote-down");
		System.out.println("userTwelveVoteDownPair: " + this.userTwelveVoteDownPair.count());
		
		this.userTwelveFavoritePair = createTwelveElementPair("originalData/attributeData/favorite");
		System.out.println("userTwelveFavoritePair: " + this.userTwelveFavoritePair.count());
		
		this.userTwelveLetterPair = createTwelveElementPair("originalData/attributeData/Letter");
		System.out.println("userTwelveFavoritePair: "  +this.userTwelveLetterPair.count());
		
	  writeDataFile("originalData/predictionDataVersion3"); 
*/
	}	
	public JavaPairRDD<String, String> createTwelveElementPair(String txtPath){
		//读取【用户id  12个每月数据】文件，返回【训练集用户id：每月数据】键值对
		JavaRDD<String> input = sc.textFile(txtPath);
		JavaPairRDD<String, String> pair = input.mapToPair(new pair()).filter(new pickTrainedUser());
		return pair;
	}
	public static class pair implements PairFunction<String, String, String>{
		public Tuple2<String, String> call(String s){
			return new Tuple2<String, String>(s.split(" ")[0], s.substring(9));
		}
	}
	public JavaPairRDD<String, String> createUserWordsPair(){
		JavaPairRDD<String, String> blogUserPair = createBlogPair("originalData/2_Post.txt", false) ; //blogID: userID
		//System.out.println("blogId: userID " + blogUserPair.count());  //1000000
		JavaPairRDD<String, String> blogWordsPair = createBlogPair("originalData/contentLength.txt", true) ; //blogID: words
		//System.out.println("blogId: words " + blogWordsPair.count()); //1000000
		JavaPairRDD<String, Iterable<String>> blogPair = blogUserPair.union(blogWordsPair).groupByKey();
		//System.out.println("unionAndGroupByKey:  " + blogPair.count()); //1000000
		JavaPairRDD<String, String> userWordsPair = blogPair.values().mapToPair(new createUserWordsPair());
		JavaPairRDD<String, String> trainedUserWords = userWordsPair.filter(new pickTrainedUser());	//17140
	   //long trainedUserNum = trainedUserWords.groupByKey().count();
		//System.out.println(trainedUserNum);//795
		//JavaPairRDD<String, String> result = trainedUserWords.reduceByKey(new 	calculateTotalWords() );这一句计算得出的是年总书写字数
		AvgCount inital = new AvgCount(0, 0);
		JavaPairRDD<String, AvgCount> avgCounts = trainedUserWords.combineByKey(createAcc, addAndCount, combine);
		JavaPairRDD<String, String> result = avgCounts.mapValues(new userAvgWords());
		return result;
	}
	public static class userAvgWords implements Function<AvgCount, String>{
		public String call( AvgCount av){
			return String.valueOf( av.avg() );
		}
	}
	public static class AvgCount implements Serializable{
		public int total;
		public int num;
		public AvgCount(int to, int n){
			this.total = to;
			this.num = n;
		}
		public int avg(){
			return (int) Math.round(total * 1.0 / num);
		}
	}
	static Function<String, AvgCount> createAcc = new Function<String, AvgCount>(){
		public AvgCount call(String word){
			return new AvgCount(Integer.valueOf(word), 1);
		}
	};
	static Function2<AvgCount, String, AvgCount> addAndCount = 
			new Function2<AvgCount, String, AvgCount>(){
		public AvgCount call(AvgCount a, String str){
			a.total += Integer.valueOf(str);
			a.num += 1;
			return a;
		}
	};
	static Function2<AvgCount, AvgCount, AvgCount> combine = 
			new Function2<AvgCount, AvgCount, AvgCount>(){
		public AvgCount call(AvgCount a, AvgCount b){
			a.total += b.total;
			a.num += b.num;
			return a;
		}
	};
	
	public static class calculateTotalWords implements  Function2<String, String, String> {
			public String call(String strNum1, String strNum2){
				return String.valueOf((Integer.parseInt(strNum1) + Integer.parseInt(strNum2)) );
			}				
	}
 	public static class pickTrainedUser implements Function<Tuple2<String, String>, Boolean>{
		//用以挑选训练集用户，楼下有一个choiceTrainedUser()方法，同样挑选训练集用户，但是其作用对象是【字符串：整数】键值对
		public Boolean call(Tuple2<String, String> tup){
			if(trainedUserIDList.contains(tup._1()))
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
	public void printStrIntPair(JavaPairRDD<String, Integer> pairRDD){
		//打印【字符串，整数】键值对链表
		List<Tuple2<String, Integer>> list = pairRDD.collect();
		for(Tuple2<String, Integer> tup: list){
			System.out.println(tup._1() + "  " + tup._2());
		}
	}
	public JavaPairRDD<String, Integer> dailyMonthlyNums (String path, int timeEndIndex){
		//给出文件路径和截取的时间下标（可以决定是取月份还是日子），得到【用户时间：总数】键值对，用户是具有成长值的训练集用户
		JavaRDD<String> input = this.sc.textFile(path);
		// [ userIdTime: 1]
		JavaPairRDD<String, Integer> userIDTimeOnePair = input.mapToPair(new createIdTimeOnePair(timeEndIndex));

		// [ userIdTime: totalNum ]
		JavaPairRDD<String, Integer> userIDTimeNumsPair = userIDTimeOnePair.reduceByKey(new createIdTimeNumPair());
		JavaPairRDD<String, Integer> trainedUserPair = userIDTimeNumsPair.filter(new choiceTrainedUser());
		List<Tuple2<String, Integer>> tups = trainedUserPair.collect();
//		for(Tuple2<String, Integer> tup: tups)
//			System.out.println(tup._1() + "   " + tup._2());
		if(tups.size() == 0) System.out.println("filtedUserList is empty");
		return trainedUserPair;
	}
 	public static class choiceTrainedUser implements Function<Tuple2<String, Integer>, Boolean>{
		//用以挑选训练集用户
		public Boolean call(Tuple2<String, Integer> tup){
			if(trainedUserIDList.contains(tup._1().substring(0, 8)))
				return true;
			else return false;
		}
	}

	public JavaPairRDD<String, Integer> userIDActiveDayPair(String path, int timeEndIndex, int activeNum){
		//给出文件路径，截取时间下标和划定为活跃的数目，返回 【训练集用户id: 活跃天数】
		JavaRDD<String> input = this.sc.textFile(path);
		// [ userIdTime: 1]
		JavaPairRDD<String, Integer> userIDTimeOnePair = input.mapToPair(new createIdTimeOnePair(timeEndIndex));
		// [ userIdTime: totalNum ]
		JavaPairRDD<String, Integer> userIDTimeNumsPair = userIDTimeOnePair.reduceByKey(new createIdTimeNumPair());
		// [ idTime: month/day'sNum ]    选用trainedActiveDay()过滤方法则得到训练集用户的活跃键值对。
		JavaPairRDD<String, Integer> activePostDay = userIDTimeNumsPair.filter(new trainedActiveDay(activeNum));
		// [ id: activePostDaysNum ]
		JavaPairRDD<String, Integer> idActivePostDay = activePostDay.keys().mapToPair(new createIdOneActPair()).reduceByKey((new createIdActPostPair()));
	   return idActivePostDay;
	}
	public static class createIdActPostPair implements Function2<Integer, Integer, Integer>{
		public Integer call(Integer a, Integer b){
			return (a + b);
		}
	}
	public static class createIdOneActPair implements PairFunction<String, String, Integer>{
		public Tuple2<String, Integer> call(String idTime){
			String id  = idTime.split("-")[0];
			return new Tuple2<String, Integer>(id, 1);
		}
	}
	public static class trainedActiveDay implements Function<Tuple2<String, Integer>, Boolean>{
		//挑选：一是训练集用户，二天数到达活跃要求 的键值对。
		private int activeNum;
		public trainedActiveDay(int num){
			this.activeNum = num;
		}
		public Boolean call(Tuple2<String, Integer> tup){
			if(trainedUserIDList.contains(tup._1().substring(0, 8)) && (tup._2() >= this.activeNum) ){
				return true;
		}else return false;
	}
	}
	public static class choiceActiveDay implements Function<Tuple2<String, Integer>, Boolean>{
		//挑选 天数达到活跃要求的键值对，没有对用户进行筛选
		private int activeNum ;
		public choiceActiveDay(int num){
			this.activeNum = num;
		}
		public Boolean call(Tuple2<String, Integer> tup){
			if(tup._2 >= this.activeNum){
				return true;
			}else{
				return false;
			}
		}
	}
	public static class createIdTimeNumPair implements Function2<Integer, Integer, Integer>{
		//用以计数
		public Integer call(Integer a, Integer b){
			return (a + b);
		}
	}
	public static class createIdTimeOnePair implements PairFunction<String, String, Integer>{
		//给出截取时间下标，可以决定是取到月份还是具体到日子，返回【用户id+时间：1】的键值对
		private int timeEndIndex;
		public createIdTimeOnePair(int index){
			this.timeEndIndex = index;
		}
		public Tuple2<String, Integer> call(String line){
			String[] splitedArr = line.split("\001");
			String idTime = splitedArr[0] +"-" +  splitedArr[2].substring(0, this.timeEndIndex);
			return new Tuple2<String, Integer> (idTime, 1);
		}
	}
	public void writeDataFile(String finalResultPath){
		//把数据写入文件
		Map<String, String> twelvePostMap = this.userTwelvePostPair.collectAsMap();
		Map<String, String> twelveBrowseMap = this.userTwelvePostPair.collectAsMap();
		Map<String, String> twelveComMap = this.userTwelvePostPair.collectAsMap();
		Map<String, String> twelveVoteUpMap = this.userTwelvePostPair.collectAsMap();
		Map<String, String> twelveVoteDownMap = this.userTwelvePostPair.collectAsMap();
		Map<String, String> twelveFavoriteMap = this.userTwelvePostPair.collectAsMap();
		Map<String, String> twelveLetterMap = this.userTwelvePostPair.collectAsMap();
		
		File file = new File(finalResultPath);
		try{
			//DecimalFormat df = new DecimalFormat("#.####");
			BufferedWriter bw = new BufferedWriter(new FileWriter(file, true));
			for(ArrayList<String> aUser: this.userInforList){
				String userId = aUser.get(0); //用户编号
				
				String twelvePost = twelveElementStr(twelvePostMap, userId);
				String twelveBrowse = twelveElementStr(twelveBrowseMap, userId);
				String twelveCom = twelveElementStr(twelveComMap, userId);
				String twelveVoteUp = twelveElementStr(twelveVoteUpMap, userId);
				String twelveVoteDown = twelveElementStr(twelveVoteDownMap, userId);
				String twelveFavorite = twelveElementStr(twelveFavoriteMap, userId);
				String twelveLetter = twelveElementStr(twelveLetterMap, userId);
				
				String userDescribeStr = aUser.get(1) + ", " // 票房
						+ aUser.get(2) + " "//年书写总字数 
						+ aUser.get(3) + " " //发表活跃月数目
						+ aUser.get(4) + " "//浏览活跃日数目
						+  aUser.get(5)+ " " //评论
						+  aUser.get(6) + " "//点赞
						+  aUser.get(7) + " " 	//点踩
						+  aUser.get(8) + " "//喜欢
						+  aUser.get(9) + " " //关注
						+ aUser.get(10) + " " //私信
						+ twelvePost + " " + twelveBrowse + " " + twelveCom + " "+  twelveVoteUp+ " " +  twelveVoteDown + " "
						+ twelveFavorite + " "+  twelveLetter;
				
				bw.write(userDescribeStr);
				bw.write("\n");
			}
			bw.close();
			System.out.println("ok");
		}catch(IOException e){
			e.printStackTrace();
		}
	}
	public String twelveElementStr(Map<String, String> twelveMap, String id){
		
		String line = "";
		if(twelveMap.containsKey(id)){
			String[] arr = twelveMap.get(id).split(" ");
			if(arr.length == 12){
				for(String s: arr)
					line += s + " ";
			}else{
				System.out.println("wrong ! there must be twelve elements !");			
			}
		}else{
			line = "0 0 0 0 0 0 0 0 0 0 0 0 ";
		}
		return line;
	}
	public ArrayList<ArrayList<String>>  allUserInforList(){
		//用户所有信息的链表，每个元素链表是一个用户的所有信息（发表/浏览/评论/……数目）
		ArrayList<ArrayList<String>> userList = new ArrayList<ArrayList<String>>();
//		Map<String, Integer> PostMap = this.userPostPair.collectAsMap();
//		Map<String, Integer> BrowseMap = this.userBrowsePair.collectAsMap();
		
		Map<String, String> PostWordsMap = this.userWordsPair.collectAsMap();
		
		Map<String, Integer> ActPostMonMap = this.userActPostMonPair.collectAsMap();
		Map<String, Integer> ActBrowDailyMap = this.userActBrowDailyPair.collectAsMap();
		
		Map<String, Integer> ComMap = this.userComPair.collectAsMap();
		Map<String, Integer> VoteUpMap = this.userVoteUpPair.collectAsMap();
		Map<String, Integer> VoteDownMap = this.userVoteDownPair.collectAsMap();
		Map<String, Integer> FavoriteMap = this.userFavoritePair.collectAsMap();
		Map<String, Integer> FollowMap = this.userFollowPair.collectAsMap();
		Map<String, Integer> LetterMap = this.userLetterPair.collectAsMap();
		
		for(int i = 0; i < this.trainDataList.size() - 2; i ++){
			ArrayList<String> aList = this.trainDataList.get(i);
			String userID = aList.get(0); //训练用户id
			ArrayList<String> aUser = new ArrayList<String>();
			aUser.add(userID);
			aUser.add(aList.get(1));//训练用户成长值
			//PostWords
			if(PostWordsMap.containsKey(userID)){
				aUser.add(PostWordsMap.get(userID));
			}else{
				aUser.add("0");
			}
			//PostMap
			if(ActPostMonMap.containsKey(userID)){
				aUser.add(String.valueOf( ActPostMonMap.get(userID)  ));
			}else{
				aUser.add("0");
			}
			//BrowseMap
			if(ActBrowDailyMap.containsKey(userID)){
				aUser.add(Integer.toString( ActBrowDailyMap.get(userID) ));
			}else{
				aUser.add("0");
			}
			//ComMap
			if(ComMap.containsKey(userID)){
				aUser.add(Integer.toString( ComMap.get(userID) ));
			}else{
				aUser.add("0");
			}
			//VoteUpMap
			if(VoteUpMap.containsKey(userID)){
				aUser.add(Integer.toString( VoteUpMap.get(userID) ));
			}else{
				aUser.add("0");
			}
			//VoteDownMap
			if(VoteDownMap.containsKey(userID)){
				aUser.add(Integer.toString( VoteDownMap.get(userID) ));
			}else{
				aUser.add("0");
			}
			//FavoriteMap
			if(FavoriteMap.containsKey(userID)){
				aUser.add(Integer.toString( FavoriteMap.get(userID) ));
			}else{
				aUser.add("0");
			}
			//FollowMap
			if(FollowMap.containsKey(userID)){
				aUser.add(Integer.toString( FollowMap.get(userID) ));
			}else{
				aUser.add("0");
			}
			//LetterMap
			if(LetterMap.containsKey(userID)){
				aUser.add(Integer.toString( LetterMap.get(userID) ));
			}else{
				aUser.add("0");
			}
			//
			if(aUser.size() == 11){
				userList.add(aUser);
			}else{
				System.out.println("It is no 11 elements, wrong ! " + userID);
			}
		}
		System.out.println("in allUserInforList(), there are " + userList.size() + "  users"); /**/
		return userList;
	}

	public ArrayList<ArrayList<String>> trainDataList (String trainDataPath){
		//将训练数据变成链表，每一个链表元素为一个用户及其成长值
		ArrayList<ArrayList<String>> totalList = new ArrayList<ArrayList<String>>();
		ArrayList<String> idList = new ArrayList<String>();
		File file = new File(trainDataPath);
		try{
			BufferedReader br = new BufferedReader(new FileReader(file));
			String line;
			while((line = br.readLine()) != null){
				ArrayList<String> aList = new ArrayList<String>();
				String[] splitedLine = line.split("\001");
				idList.add(splitedLine[0]);
				for(String word: splitedLine)
					aList.add(word);
				totalList.add(aList);
			}
			totalList.add(idList);
			br.close();
		}catch(IOException e){
			e.printStackTrace();
		}
		return totalList;
	}
	public JavaPairRDD<String, Integer> calculateTotalNum(String path, int userIndex){
		//给出（发表/收藏/点赞等）文件路径和用户id的下标，创建【训练集用户编号：（发表/收藏/……）数目】键值对
		JavaRDD<String> input = sc.textFile(path);
		JavaPairRDD<String, Integer> userOnePair = input.mapToPair(new createPair(userIndex));
		JavaPairRDD<String, Integer> userNumPair = userOnePair.reduceByKey(new userNumPair());
		JavaPairRDD<String, Integer> trainedUseNumPair = userNumPair.filter(new choiceTrainedUser());
		return userNumPair;		
	}
	static class userNumPair implements Function2<Integer, Integer, Integer>{
		//用以计算用户数目
		public Integer call(Integer a, Integer b){
			return (a + b);
		}
	}
	static class createPair implements PairFunction<String, String, Integer>{
		//用以创建【用户编号：1】的键值对，计算数目
		private int userIndex;
		public createPair(int index){
			this.userIndex = index;
		}
		public Tuple2<String, Integer> call(String record){
			return new Tuple2<String, Integer>(record.split("\001")[this.userIndex], 1);
		}
	}
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		CreateData test = new CreateData();
		
	}

}
