package edu.umkc.pb;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import javax.servlet.RequestDispatcher;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import scala.Tuple2;

/**
 * Servlet implementation class CountryServlet
 */
@WebServlet("/country")
public class CountryServlet extends HttpServlet {
	private static final long serialVersionUID = 1L;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public CountryServlet() {
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
        String outputFile = getServletContext().getRealPath("/")
	            +"/flare.json";
        File f = new File(outputFile);
        BufferedWriter bw;
        FileWriter fw = null;
        System.out.println("-----------" + inputFile);
        SparkConf sparkConf = new SparkConf().setAppName("Country").setMaster("local").set("spark.driver.allowMultipleContexts", "true");
        SparkContext ctx = new SparkContext(sparkConf);
        SQLContext sc = new SQLContext(ctx);
        DataFrame d = sc.jsonFile(inputFile);
        d.registerTempTable("tweets");
        DataFrame data = sc.sql("select place.country from tweets");
        JavaPairRDD<String,Integer> ones = data.toJavaRDD().mapToPair(
                new PairFunction<Row, String, Integer>()
        {
            public Tuple2<String, Integer> call(Row row){
            	String i = row.getString(0);
                return new Tuple2<>(i,1);
            }
        });
        JavaPairRDD<String, Integer> counts = ones.reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer i1, Integer i2) {
                        return i1 + i2;
                    }
                });

        JavaPairRDD<String, Integer> reduced = counts.mapToPair(
                new PairFunction<Tuple2<String, Integer>, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                        if(stringIntegerTuple2._2() >= 200)
                            return new Tuple2<>(stringIntegerTuple2._1(),stringIntegerTuple2._2());
                        else
                            return new Tuple2<>("others",stringIntegerTuple2._2());
                    }
                }
        );
        JavaPairRDD<String, Integer> countsReduced = reduced.reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer i1, Integer i2) {
                        return i1 + i2;
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
        bw.append("{ \"children\":[");
        List<String> keys = countsReduced.keys().toArray();
        List<Integer> values = countsReduced.values().toArray();
        for(int i=0;i<keys.size();i++)
        {
        	if(keys.get(i).equals("????"))
        		keys.set(i, "India");
        	else if(keys.get(i).equals("???? ??????"))
        		keys.set(i,"Saudi Arabia");
        	else if(keys.get(i).equals("??"))
        		keys.set(i, "Japan");
        	else if(keys.get(i).equals("???"))
        		keys.set(i, "Singapore");
        	else if(keys.get(i).equals("?????????"))
        		keys.set(i, "Thailand");
        	else if(keys.get(i).equals("??????"))
        		keys.set(i, "Rusia");
        	else if(keys.get(i).equals("???????"))
        		keys.set(i, "China");
        	else if(keys.get(i).equals("???????? ??????? ???????"))
        		keys.set(i, "United Arab Emirates");
        	else if(keys.get(i).equals("??????? ??????? ????????"))
        		keys.set(i, "Saudi Arabia");
        }
        for(int i = 0; i < keys.size()-1;i++)
        {
        	bw.append("{\"name\": \""+keys.get(i)+"\", \"size\":"+ values.get(i) +"},");
        	System.out.println("{\"name\": \""+keys.get(i)+"\", \"size\":"+ values.get(i) +"},");
        }
        bw.append("{\"name\": \""+keys.get(keys.size()-1)+"\", \"size\":"+ values.get(values.size()-1) +"}");
        bw.append("]}");
        bw.flush();
        bw.close();
       // countsReduced.saveAsTextFile(outputFile);
        ctx.stop();
        request.setAttribute("keys", keys);
        request.setAttribute("values", values);
        
        RequestDispatcher rd = request.getRequestDispatcher("graph.html");
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
