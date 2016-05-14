<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
    pageEncoding="ISO-8859-1"%>
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
	<title>D3 CSV</title>

</head>
<body>
<script src="http://d3js.org/d3.v3.min.js"></script>
<script src="d3.layout.cloud.js"></script>
<script type="text/javascript">

var fill = d3.scale.category20();

var cityData = [],
	cityPop = [], 
    width = 3000, 
    height = 1800;

d3.csv("word.csv", function(data) {
    // build the list of city names
    data.forEach( function (d) {
        cityData.push(d.word);
       // console.log(d.population);
        cityPop.push(d.count);
    });

    d3.layout.cloud().size([500, 500])
        .words(cityData.map(function(_,i) {
       // console.log(cityData[i] + " " + cityPop[i] );
            return {text: cityData[i], size:10 + cityPop[i] / 100};
        }))
        .rotate(function() { return ~~(Math.random() * 2) * 90; })
        .font("Impact")
        .fontSize(function(d) { return d.size; })
        .on("end", draw)
        .start();

});

function draw(words) {
d3.select("body").append("svg")
    .attr("width", 2000)
    .attr("height", 1200)
    .append("g")
    .attr("transform", "translate(400,400	)")
    .selectAll("text")
    .data(words)
    .enter().append("text")
    .style("font-size", function(d) { 
    console.log(words);
    return d.size+"px"; })
    .style("font-family", "Impact")
    .style("fill", function(d, i) { return fill(i); })
    .attr("text-anchor", "middle")
    .attr("transform", function(d) {
        return "translate(" + [d.x, d.y] + ")rotate(" + d.rotate + ")";
    })
    .text(function(d) { return d.text; });
}	
</script>
</body>
</html>