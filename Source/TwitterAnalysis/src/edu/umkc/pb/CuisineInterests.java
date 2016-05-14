package edu.umkc.pb;

import java.io.IOException;
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
 * Servlet implementation class CuisineInterests
 */
@WebServlet("/cuisine")
public class CuisineInterests extends HttpServlet {
	private static final long serialVersionUID = 1L;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public CuisineInterests() {
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
        SparkConf sparkConf = new SparkConf().setAppName("CuisineCounts").setMaster("local").set("spark.driver.allowMultipleContexts", "true");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        SQLContext sc = new SQLContext(ctx);

        DataFrame d = sc.jsonFile(inputFile);
        d.registerTempTable("tweets");
        DataFrame data,data1;
        data = sc.sql("select text from tweets where place.country='United States'");
        data.registerTempTable("texts");
        data1 = sc.sql("select text from tweets where text LIKE '%indian%' or text LIKE '%Indian%' or text LIKE '%thai%' or text LIKE '%japanese%'or text LIKE '%mexican%' or text LIKE '%Mexican%' or text LIKE '%italian%' or text LIKE '%Italian%' or text LIKE '%chinese%' or text LIKE '%Chinese%' or text LIKE '%american%' or text LIKE '%American%'");
        long total = data1.count();
        System.out.println(data1.count() +" " + data.count());
        JavaRDD<String> words = data1.toJavaRDD().flatMap(
                new FlatMapFunction<Row, String>() {
                    @Override
                    public Iterable<String> call(Row row) throws Exception {
                        String s = "";
                        if(row.getString(0).contains("indian") || row.getString(0).contains("Indian") || row.getString(0).contains("italian") || row.getString(0).contains("Italian") || row.getString(0).contains("mexican") || row.getString(0).contains("Mexican") || row.getString(0).contains("american") || row.getString(0).contains("American") || row.getString(0).contains("Chinese") || row.getString(0).contains("chinese")) {
                            if (row.getString(0).contains("indian") || row.getString(0).contains("Indian"))
                                s = "Indian";
                            if (row.getString(0).contains("italian") || row.getString(0).contains("Italian"))
                                s = "Italian";
                            if (row.getString(0).contains("mexican") || row.getString(0).contains("Mexican"))
                                s = "Mexican";
                            if (row.getString(0).contains("chinese") || row.getString(0).contains("Chinese") )
                                s = "Chinese";
                            if (row.getString(0).contains("american") || row.getString(0).contains("American") )
                                s = "American";
                        }
                        else
                            s ="Others";
                        s.trim();
                        System.out.println(s);
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
        request.setAttribute("total", total);
        request.setAttribute("keys", keys);
        request.setAttribute("values", values);
        RequestDispatcher rd = request.getRequestDispatcher("cuisine.jsp");
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
