<%@page import="java.util.List"%>
<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
	pageEncoding="ISO-8859-1"%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>

<meta http-equiv="Content-Type" content="text/html; charset=ISO-8859-1">
<script src="https://code.highcharts.com/highcharts.js"></script>
<script src="https://code.highcharts.com/modules/exporting.js"></script>
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
        console.log(<%=keys.get(i)%>);
        series.push(['<%=keys.get(i)%>', <%=values.get(i)%>]);
    <%}%>
    console.log(series);
 </script>

</head>
<body>
	<div id="container" style="width: 550px; height: 400px; margin: 0 auto"></div>
</body>
<script language="JavaScript">

    $(document).ready(function() {
    
    chart = new Highcharts.Chart({
        chart: {
            renderTo: 'container',
            type: 'pie'
        },
        
        plotOptions: {
            pie: {
                showInLegend: false,
                dataLabels: {
                    formatter: function(){
                        console.log(this);
                             this.point.visible = true;
                            return this.key;
                        }
					   }
                    }
                
        },
        title:
        {
        	text:'People Tweeting from Different Countries'
        }, 
        series: [{
            data: [
            	<%for (int i = 0; i < keys.size() - 1; i++) {%>
        {name:'<%=keys.get(i)%>', y:<%=values.get(i)%>},
    <%}%>
                {name:'<%=keys.get(keys.size() - 1)%>', y:<%=values.get(values.size() - 1)%>}
            ]
        }]
    });
});
</script>
</html>