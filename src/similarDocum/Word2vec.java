package similarDocum;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.feature.Word2Vec;
import org.apache.spark.ml.feature.Word2VecModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class Word2vec {

	/**
	 * @param args
	 */
	SparkConf conf;
	JavaSparkContext sc;
	SQLContext sql;
	String allWords;
	ArrayList<Double> resultList;

	public Word2vec(String path){
		this.conf = new SparkConf().setAppName("word2vecSimilar").setMaster("local");
		this.sc = new JavaSparkContext(conf);
		this.sql = new SQLContext(sc);	
		this.allWords = readFile(path);
		this.resultList = word2VecCalcul();
	}
	public ArrayList<Double> word2VecCalcul(){
		ArrayList<Double> doubleList = new ArrayList<Double>();
		List<Row> data = Arrays.asList(
				RowFactory.create(Arrays.asList(this.allWords.split(" "))));
		StructType schema = new StructType(new StructField[]{
				new StructField("text", new ArrayType(DataTypes.StringType, true), false, Metadata.empty())
		});
		DataFrame documentDF = sql.createDataFrame(data, schema);
		Word2Vec word2vec = new Word2Vec()
		.setInputCol("text")
		.setOutputCol("result")
		.setVectorSize(3)
		.setMinCount(0);
		
		Word2VecModel model = word2vec.fit(documentDF);
		DataFrame result = model.transform(documentDF);
		for(Row row: result.collect()){
			//List<String> text = row.getList(0);
			Vector vector = (Vector) row.get(1);
			double[] doubleArr = vector.toArray();
			for(double d: doubleArr)
				doubleList.add(d);
		}
		sc.stop();
		return doubleList;
	}
	public String readFile(String path){
		File file = new File(path);	
		String allWords = "";
		try{
			BufferedReader br = new BufferedReader(new FileReader(file));
			String str = br.readLine();		
			while( (str = br.readLine()) != null ){
				if(str != "")
					allWords += str  + " ";
			}
		}catch(IOException e){
			e.printStackTrace();
		}	
		return allWords;
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		//Word2vec test = new Word2vec();
	}

}
