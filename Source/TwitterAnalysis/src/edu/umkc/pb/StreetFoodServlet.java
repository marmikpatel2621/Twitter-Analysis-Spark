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
 * Servlet implementation class StreetFoodServlet
 */
@WebServlet("/StreetFoodServlet")
public class StreetFoodServlet extends HttpServlet {
	private static final long serialVersionUID = 1L;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public StreetFoodServlet() {
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
        DataFrame data = sc.sql("select text from tweets where text LIKE '%chat%' or text LIKE '%chat%' or text LIKE '%pizza%' or text LIKE '%samosa%' or text LIKE '%vada pav%' or text LIKE '%pani puri%' or text LIKE '%hot dog%' or text LIKE '%burger%' or text LIKE '%burrito%' or text LIKE '%donut%'");
        System.out.println("++++++++++"+data.count());
        JavaRDD<String> words = data.toJavaRDD().flatMap(
                new FlatMapFunction<Row, String>() {
                    @Override
                    public Iterable<String> call(Row row) throws Exception {
                        String s = "";
                        if(row.getString(0).contains("chat") || row.getString(0).contains("Chat")) {
                            s= s + " " +"chat";

                        }
                        if(row.getString(0).contains("pizza") || row.getString(0).contains("Pizza")) {
                            s= s + " " +"pizza";

                        }

                        if(row.getString(0).contains("samosa") || row.getString(0).contains("Samosa")) {
                            s= s + " " +"Samosa";

                        }
                        if(row.getString(0).contains("pani puri") || row.getString(0).contains("Pani puri")) {
                            s= s + " " +"PaniPuri";

                        }
                        if(row.getString(0).contains("vada pav") || row.getString(0).contains("Vada pav")) {
                            s= s + " " +"Vadapav";

                        }
                        if(row.getString(0).contains("hot dog") || row.getString(0).contains("Hotdog")) {
                            s= s + " " +"HotDog";

                        }
                        if(row.getString(0).contains("burger") || row.getString(0).contains("Burger")) {
                            s= s + " " +"Burger";

                        }
                        if(row.getString(0).contains("burrito") || row.getString(0).contains("Burrito")) {
                            s= s + " " +"Burrito";

                        }
                        if(row.getString(0).contains("donut") || row.getString(0).contains("Donut")) {
                            s= s + " " +"donut";

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
       // request.setAttribute("total", total);
        request.setAttribute("keys", keys);
        request.setAttribute("values", values);
        RequestDispatcher rd = request.getRequestDispatcher("street.jsp");
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
