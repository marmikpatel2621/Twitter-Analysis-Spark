<%@page import="java.util.List"%>
<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
	pageEncoding="ISO-8859-1"%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>

<meta http-equiv="Content-Type" content="text/html; charset=ISO-8859-1">
<script src="https://www.amcharts.com/lib/3/amcharts.js"></script>
<script src="https://www.amcharts.com/lib/3/pie.js"></script>
<script src="https://www.amcharts.com/lib/3/themes/light.js"></script>
<style>
#chartdiv {
	width		: 100%;
	height		: 435px;
	font-size	: 11px;
}																					
</style>
<script type="text/javascript"
	src="http://code.jquery.com/jquery-1.7.1.min.js"></script>
<title>Insert title here</title>
<%
	List<String> keys = (List<String>) request.getAttribute("keys");
	List<Integer> values = (List<Integer>) request.getAttribute("values");
%>
<script language="JavaScript">
    var series = [];
    <%for (int i = 0; i < keys.size(); i++) {%>
        series.push(['<%=keys.get(i)%>', <%=values.get(i)%>]);
    <%}%>
    console.log(series);
 </script>

</head>
<body>
	<div id="chartdiv"></div>
<div class="container-fluid">
  <div class="row text-center" style="overflow:hidden;">
		<div class="col-sm-3" style="float: none !important;display: inline-block;">
			<label class="text-left">Angle:</label>
			<input class="chart-input" data-property="angle" type="range" min="0" max="60" value="30" step="1"/>	
		</div>

		<div class="col-sm-3" style="float: none !important;display: inline-block;">
			<label class="text-left">Depth:</label>
			<input class="chart-input" data-property="depth3D" type="range" min="1" max="25" value="10" step="1"/>
		</div>
		<div class="col-sm-3" style="float: none !important;display: inline-block;">
			<label class="text-left">Inner-Radius:</label>
			<input class="chart-input" data-property="innerRadius" type="range" min="0" max="80" value="0" step="1"/>
		</div>
	</div>
</div>																						
</body>
<script language="JavaScript">
var chart = AmCharts.makeChart( "chartdiv", {
  "type": "pie",
  "theme": "light",
  "dataProvider": [ <%for (int i = 0; i < keys.size() - 1; i++) {%>
        {"title":"<%=keys.get(i)%>", "value":<%=values.get(i)%>},
    <%}%>
                {"title":"<%=keys.get(keys.size() - 1)%>", "value":<%=values.get(values.size() - 1)%>}
              ],
  "valueField": "value",
  "titleField": "country",
  "outlineAlpha": 0.4,
  "depth3D": 15,
  "balloonText": "[[title]]<br><span style='font-size:14px'><b>[[value]]</b> ([[percents]]%)</span>",
  "angle": 30,
  "export": {
    "enabled": true
  }
} );
jQuery( '.chart-input' ).off().on( 'input change', function() {
  var property = jQuery( this ).data( 'property' );
  var target = chart;
  var value = Number( this.value );
  chart.startDuration = 0;

  if ( property == 'innerRadius' ) {
    value += "%";
  }

  target[ property ] = value;
  chart.validateNow();
} );
</script>
</html>