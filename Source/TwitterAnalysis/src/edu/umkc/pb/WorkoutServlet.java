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
 * Servlet implementation class WorkoutServlet
 */
@WebServlet("/workout")
public class WorkoutServlet extends HttpServlet {
	private static final long serialVersionUID = 1L;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public WorkoutServlet() {
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
        DataFrame data = sc.sql("select text from tweets where text LIKE '%gym%' or text LIKE '%Gym%' or text LIKE '%yoga%'or text LIKE '%Yoga%' or text LIKE '%swimming%' or text LIKE '%Swimming%' or text LIKE '%yog%' or  text LIKE '%Yog%' or text LIKE '%Aerobics%' or text LIKE '%aerobics%' or text LIKE '%zumba%'");
        long total = data.count();
        System.out.println(total);
        JavaRDD<String> words = data.toJavaRDD().flatMap(
                new FlatMapFunction<Row, String>() {
                    @Override
                    public Iterable<String> call(Row row) throws Exception {
                        String s = "";
                        if(row.getString(0).contains("gym") || row.getString(0).contains("zumba") || row.getString(0).contains("yoga") || row.getString(0).contains("aerobics") || row.getString(0).contains("Yoga") || row.getString(0).contains("Yog") || row.getString(0).contains("yog") || row.getString(0).contains("Aerobics")) {
                            if (row.getString(0).contains("zumba"))
                                s = s+" " +"zumba";
                            if (row.getString(0).contains("yoga") || row.getString(0).contains("Yoga") || row.getString(0).contains("Yog") || row.getString(0).contains("yog"))
                                s = s + " " + "Yoga";
                            if (row.getString(0).contains("aerobics") || row.getString(0).contains("Aerobics"))
                                s = s + " " +"Aerobics";
                            if (row.getString(0).contains("gym"))
                                s = s + " " +"gym";

                        }
                        else
                           s = s + " " + "Others";
                        s=s.trim();
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
        RequestDispatcher rd = request.getRequestDispatcher("workout.jsp");
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
