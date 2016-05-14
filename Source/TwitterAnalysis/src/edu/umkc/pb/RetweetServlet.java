package edu.umkc.pb;

import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.List;

import javax.servlet.RequestDispatcher;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import scala.Tuple2;

/**
 * Servlet implementation class RetweetServlet
 */
@WebServlet("/RetweetServlet")
public class RetweetServlet extends HttpServlet {
	private static final long serialVersionUID = 1L;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public RetweetServlet() {
        super();
        // TODO Auto-generated constructor stub
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		String inputFile = getServletContext().getRealPath("/")
	            +"/Twitter2.json";
        String outputFile = "G:\\StudyMaterial\\output";
        SparkConf sparkConf = new SparkConf().setAppName("WorkoutCounts").setMaster("local").set("spark.driver.allowMultipleContexts", "true");
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        SQLContext sc = new SQLContext(ctx);

        DataFrame d = sc.jsonFile(inputFile);
        d.registerTempTable("tweets");
        DataFrame data = sc.sql("select text from tweets where quoted_status.retweet_count>0 and text LIKE '%foodie%' or text LIKE '%protein%' or text LIKE '%salads%' or text LIKE '%pizza%' or text LIKE '%burger%' or text LIKE '%gym%' or text LIKE '%health%' or text LIKE '%diet%' or text LIKE '%fitness%' or text LIKE '%exercise%' or text LIKE '%body building%' or text LIKE '%weight loss%' or text LIKE '%workout%' ");

        System.out.println("++++++++++"+data.count());
        JavaRDD<String> words = data.toJavaRDD().flatMap(
                new FlatMapFunction<Row, String>() {
                    @Override
                    public Iterable<String> call(Row row) throws Exception {
                        String s = "";
                        if(row.getString(0).contains("foodie") || row.getString(0).contains("protein") || row.getString(0).contains("salads") || row.getString(0).contains("pizza") || row.getString(0).contains("burger")) {
                            s= s + " " +"FoodLovers";

                        }
                        if(row.getString(0).contains("gym") || row.getString(0).contains("body building") || row.getString(0).contains("fitness") || row.getString(0).contains("diet") || row.getString(0).contains("health") || row.getString(0).contains("weight loss")) {
                            s= s + " " +"FitnessFreaks";

                        }
                        else{

                            s = s + " " + "Others";


                        }


                        s = s.trim();
                        return Arrays.asList(s.split(" "));
                    }
                }
        );
        JavaPairRDD<String, Integer> counts = words.mapToPair(
                new PairFunction<String, String, Integer>(){
                    public Tuple2<String, Integer> call(String s){
                        return new Tuple2(s, 1);
                    }
                } );

        // Java 7 and earlier: count the words
        JavaPairRDD<String, Integer> reducedCounts = counts.reduceByKey(
                new Function2<Integer, Integer, Integer>(){
                    public Integer call(Integer x, Integer y){ return x + y; }
                } );	
        List<String> keys = reducedCounts.keys().toArray();
        List<Integer> values = reducedCounts.values().toArray();
        ctx.stop();
 //       request.setAttribute("total", total);
        request.setAttribute("keys", keys);
        request.setAttribute("values", values);
        RequestDispatcher rd = request.getRequestDispatcher("retweet.jsp");
        rd.forward(request, response);
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		doGet(request, response);
	}

}
