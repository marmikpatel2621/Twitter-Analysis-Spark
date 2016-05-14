package edu.umkc.pb;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
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
 * Servlet implementation class WordCloudServlet
 */
@WebServlet("/wordcloud")
public class WordCloudServlet extends HttpServlet {
	private static final long serialVersionUID = 1L;

	/**
	 * @see HttpServlet#HttpServlet()
	 */
	public WordCloudServlet() {
		super();
		// TODO Auto-generated constructor stub
	}

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse
	 *      response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		// TODO Auto-generated method stub
		String inputFile = getServletContext().getRealPath("/")
	            +"/Twitter2.json";
		String outputFile = getServletContext().getRealPath("/") + "/word.csv";
		File f = new File(outputFile);
		BufferedWriter bw;
		FileWriter fw = null;
		SparkConf sparkConf = new SparkConf().setAppName("CuisineCounts").setMaster("local")
				.set("spark.driver.allowMultipleContexts", "true");
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		SQLContext sc = new SQLContext(ctx);

		DataFrame d = sc.jsonFile(inputFile);
		d.registerTempTable("tweets");
		DataFrame data, data1;
		data = sc.sql("select text from tweets where place.country='United States'");
		JavaRDD<String> words = data.toJavaRDD().flatMap(new FlatMapFunction<Row, String>() {
			@Override
			public Iterable<String> call(Row row) throws Exception {
				String s = "";
				if ((row.getString(0).contains("food") || row.getString(0).contains("Food"))) {
					s = s + " " + "Food";
				}
				if (row.getString(0).contains("health") || row.getString(0).contains("Health"))
					s = s + " " + "Health";
				if (row.getString(0).contains("vegan") || row.getString(0).contains("Vegan"))
					s = s + " " + "Vegan";
				if (row.getString(0).contains("non veg") || row.getString(0).contains("Non veg"))
					s = s + " " + "Non Veg";
				if (row.getString(0).contains("Foodlover") || row.getString(0).contains("foodlover"))
					s = s + " " + "Foodlover";
				if (row.getString(0).contains("Foodlover") || row.getString(0).contains("foodlover"))
					s = s + " " + "Foodlover";
				if (row.getString(0).contains("Foodholic") || row.getString(0).contains("foodholic"))
					s = s + " " + "Foodholic";
				if (row.getString(0).contains("Chicken") || row.getString(0).contains("chicken"))
					s = s + " " + "Chicken";
				if (row.getString(0).contains("Mutton") || row.getString(0).contains("mutton"))
					s = s + " " + "Mutton";
				if (row.getString(0).contains("Fish") || row.getString(0).contains("fish"))
					s = s + " " + "Fish";
				if (row.getString(0).contains("Beef") || row.getString(0).contains("beef"))
					s = s + " " + "Beef";
				if (row.getString(0).contains("Salad") || row.getString(0).contains("salad"))
					s = s + " " + "Salad";
				if (row.getString(0).contains("Juices") || row.getString(0).contains("juices"))
					s = s + " " + "Juices";
				if (row.getString(0).contains("Fruits") || row.getString(0).contains("fruits"))
					s = s + " " + "Fruits";
				if (row.getString(0).contains("Street food") || row.getString(0).contains("street food")
						|| row.getString(0).contains("Streetfood"))
					s = s + " " + "Street food";
				if (row.getString(0).contains("Gym") || row.getString(0).contains("gym"))
					s = s + " " + "gym";
				if (row.getString(0).contains("Yoga") || row.getString(0).contains("yoga"))
					s = s + " " + "Yoga";
				if (row.getString(0).contains("Swimming") || row.getString(0).contains("swimming"))
					s = s + " " + "Swimming";
				if (row.getString(0).contains("Zumba") || row.getString(0).contains("zumba"))
					s = s + " " + "Zumba";
				if (row.getString(0).contains("Indian") || row.getString(0).contains("indian"))
					s = s + " " + "Indian";
				if (row.getString(0).contains("Italian") || row.getString(0).contains("italian"))
					s = s + " " + "Italian";
				if (row.getString(0).contains("Mexican") || row.getString(0).contains("mexican"))
					s = s + " " + "Mexican";
				if (row.getString(0).contains("Chinese") || row.getString(0).contains("chinese"))
					s = s + " " + "Chinese";
				if (row.getString(0).contains("Thai") || row.getString(0).contains("thai"))
					s = s + " " + "Thai";
				if (row.getString(0).contains("American") || row.getString(0).contains("american"))
					s = s + " " + "American";
				if (row.getString(0).contains("Japanese") || row.getString(0).contains("japanese"))
					s = s + " " + "Japanese";
				if (row.getString(0).contains("Momos") || row.getString(0).contains("momos"))
					s = s + " " + "Momos";
				if (row.getString(0).contains("Aloo Chat") || row.getString(0).contains("aloo chat")
						|| row.getString(0).contains("aloochat") || row.getString(0).contains("Aloo chat"))
					s = s + " " + "Aloo Chat";
				if (row.getString(0).contains("Samosas") || row.getString(0).contains("samosa"))
					s = s + " " + "Samosas";
				if (row.getString(0).contains("Vada Pav") || row.getString(0).contains("vadapau")
						|| row.getString(0).contains("vada pav") || row.getString(0).contains("Vadapav"))
					s = s + " " + "Vada Pau";
				if (row.getString(0).contains("Dosa") || row.getString(0).contains("dosa"))
					s = s + " " + "Dosa";
				if (row.getString(0).contains("Pani Puri") || row.getString(0).contains("panipuri")
						|| row.getString(0).contains("Panipuri") || row.getString(0).contains("pani puri"))
					s = s + " " + "Pani puri";
				if (row.getString(0).contains("Bhel Puri") || row.getString(0).contains("bhel puri")
						|| row.getString(0).contains("bhelpuri") || row.getString(0).contains("Bhelpuri"))
					s = s + " " + "Bhel puri";
				if (row.getString(0).contains("Gym Diet") || row.getString(0).contains("Gymdiet")
						|| row.getString(0).contains("gymdiet"))
					s = s + " " + "Gym Diet";
				if (row.getString(0).contains("Diet") || row.getString(0).contains("diet"))
					s = s + " " + "Diet";
				if (row.getString(0).contains("Calories") || row.getString(0).contains("calories"))
					s = s + " " + "Calories";
				if (row.getString(0).contains("Protein") || row.getString(0).contains("protein"))
					s = s + " " + "Protein";
				if (row.getString(0).contains("Work Out") || row.getString(0).contains("work out")
						|| row.getString(0).contains("workout") || row.getString(0).contains("work"))
					s = s + " " + "Workout";
				if (row.getString(0).contains("Nutrition") || row.getString(0).contains("nutrition"))
					s = s + " " + "Nutrition";
				if (row.getString(0).contains("Exercise") || row.getString(0).contains("exercise"))
					s = s + " " + "Exercise";
				if (row.getString(0).contains("Figure") || row.getString(0).contains("figure"))
					s = s + " " + "Figure";
				if (row.getString(0).contains("Shape") || row.getString(0).contains("shape"))
					s = s + " " + "Shape";
				if (row.getString(0).contains("Fitness") || row.getString(0).contains("fitness"))
					s = s + " " + "Fitness";

				else
					s = s + " " + "Others";
				s.trim();
				return Arrays.asList(s.split(" "));
			}
		});
		JavaPairRDD<String, Integer> counts = words.mapToPair(new PairFunction<String, String, Integer>() {
			public Tuple2<String, Integer> call(String s) {
				return new Tuple2(s, 1);
			}
		});

		// Java 7 and earlier: count the words
		JavaPairRDD<String, Integer> reducedCounts = counts.reduceByKey(new Function2<Integer, Integer, Integer>() {
			public Integer call(Integer x, Integer y) {
				return x + y;
			}
		});

		if (!f.exists()) {
			try {
				f.createNewFile();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		try {
			fw = new FileWriter(f);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		bw = new BufferedWriter(fw);
		bw.append("word,count");
		List<String> keys = reducedCounts.keys().toArray();
		List<Integer> values = reducedCounts.values().toArray();
		for (int i = 0; i < keys.size() - 1; i++) {
			bw.newLine();
			bw.append(keys.get(i) + "," + values.get(i));
			System.out.println(keys.get(i) + "," + values.get(i));
		}
		bw.flush();
		bw.close();
		ctx.stop();
		// request.setAttribute("total", total);
		RequestDispatcher rd = request.getRequestDispatcher("wordcloud.jsp");
		rd.forward(request, response);
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse
	 *      response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		// TODO Auto-generated method stub
		doGet(request, response);
	}

}
