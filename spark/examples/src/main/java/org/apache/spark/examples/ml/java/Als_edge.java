
package als_Package;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.recommendation.ALS;
import org.apache.spark.ml.recommendation.ALSModel;
import org.apache.log4j.*;

import scala.collection.mutable.WrappedArray;
import java.util.*;
import java.io.Serializable;

public class Als_edge { // The message form we estimate is "user_no::product_code" for recommend
	
	private static String modelPath = "/home/ice/spark/models/recommendModel"; // have to configurate modelPath
	private static SparkSession spark;
    private static ALSModel model;
    private static StructType schema = new StructType(new StructField[]{ new StructField("user_no", DataTypes.IntegerType, false, Metadata.empty()),
																			new StructField("product_code", DataTypes.IntegerType, false, Metadata.empty())});
    private static StructType tempSchema = new StructType(new StructField[]{ new StructField("invoice_num", DataTypes.IntegerType, false, Metadata.empty())});

    public static String[] recommend(String message) { //This func recommend
    	List<Row> row = parse(message);
    	String[] rowString = row.get(0).toString().replace("[","").replace("]","").split(",");
    	String usr_no = rowString[0];
		Dataset<Row> productDF = spark.createDataFrame(row, schema);
		
		JavaRDD<Row> productSubset = model.recommendForItemSubset(productDF, 1).javaRDD();
		String[] invoiceTemp = productSubset.first().getList(1).get(0).toString().replace("[","").replace("]","").split(",");

		List<Row> invoiceRecommend = new ArrayList<>();
		invoiceRecommend.add(RowFactory.create(Integer.parseInt(invoiceTemp[0])));

		Dataset<Row> invoice = spark.createDataFrame(invoiceRecommend, tempSchema);
		JavaRDD<Row> outputRDD = model.recommendForUserSubset(invoice, 1).javaRDD();
		String[] outputProduct = outputRDD.first().getList(1).get(0).toString().replace("[","").replace("]","").split(",");
		String[] output = {usr_no, outputProduct[0]};
		return output;
    }
    
	public static List<Row> parse(String message){ //This func parse message before recommend func run 
		String[] fields = message.split("::");
		if (fields.length != 2) {
        	throw new IllegalArgumentException("Each line must contain 2 fields");
      	}
      	int user_no = Integer.parseInt(fields[0]);
      	int product_code = Integer.parseInt(fields[1]);
      	List<Row> productRow = new ArrayList<>();
		productRow.add(RowFactory.create(user_no, product_code));
		return productRow;
	}
	
	public static void setSpark(){ 
		spark = SparkSession
      	.builder()
      	.appName("JavaALSEdge")
      	.getOrCreate();

		Logger.getLogger("org").setLevel(Level.ERROR);
		Logger.getLogger("akka").setLevel(Level.ERROR);
    	model = ALSModel.load(modelPath);
    	model.setColdStartStrategy("drop");
	}
	
	public static void main(String[] args) {
		setSpark(); //Run this func just once when start the server
		String judgedMessage; //message from datafusion_system
		Scanner loopTest = new Scanner(System.in);
		
		while(true){ //This is for test
			System.out.println("Enter the message (user_no::product_code): ");
			judgedMessage = loopTest.nextLine();
			if(judgedMessage.length() <= 1) break;
    		System.out.println(recommend(judgedMessage));
		}
    	
    	
    	spark.stop();
  }
}


