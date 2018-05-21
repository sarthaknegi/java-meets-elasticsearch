package com.elasticjava_conn;

import java.awt.Color;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.*;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.util.ArrayList;
import java.util.Collection;

import javax.swing.JFrame;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.search.aggregations.metrics.avg.Avg;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PiePlot3D;
import org.jfree.data.general.DefaultPieDataset;
import org.jfree.data.general.PieDataset;
import org.jfree.util.Rotation;
import org.json.JSONException;

public class EJConn extends JFrame {
	private static final long serialVersionUID = 1L;
	String chartTitle;

	private static EJConn ejConn;

	public EJConn(String applicationTitle, String chartTitle) {
		super(applicationTitle);
		this.chartTitle = chartTitle;
	}

	public void initialize(ArrayList<Integer> listInt, ArrayList<String> listString, String chartName) throws IOException {
		// This will create the dataset
		PieDataset dataset = createDataset(listInt, listString);
		// based on the dataset we create the chart
		JFreeChart chart = createChart(dataset, chartTitle);
		// we put the chart into a panel
		ChartPanel chartPanel = new ChartPanel(chart);
		// default size
		chartPanel.setPreferredSize(new java.awt.Dimension(500, 270));
		
		chartPanel.setBackground(Color.BLACK);
		
		// add it to our application
		setContentPane(chartPanel);

		int width = 600; /* Width of the image */
		int height = 400; /* Height of the image */
		File pieChart = new File(chartName+".png");
		ChartUtilities.saveChartAsPNG(pieChart, chart, width, height);
	}

	public void initializeDouble(ArrayList<Double> listDouble, ArrayList<String> listString, String chartName) throws IOException {
		// This will create the dataset
		PieDataset dataset = createDatasetDouble(listDouble, listString);
		// based on the dataset we create the chart
		JFreeChart chart = createChart(dataset, chartTitle);
		// we put the chart into a panel
		ChartPanel chartPanel = new ChartPanel(chart);
		// default size
		chartPanel.setPreferredSize(new java.awt.Dimension(500, 270));
		
		chart.setBackgroundPaint(Color.white);
		// add it to our application
		setContentPane(chartPanel);

		int width = 600; /* Width of the image */
		int height = 400; /* Height of the image */
		File pieChart = new File(chartName+".png");
		ChartUtilities.saveChartAsPNG(pieChart, chart, width, height);
	}

	private PieDataset createDataset(ArrayList<Integer> listInt, ArrayList<String> listString) {
		DefaultPieDataset result = new DefaultPieDataset();
		for (int i = 0; i < listInt.size(); i++) {
			result.setValue(listString.get(i), listInt.get(i));
		}
		return result;

	}

	private PieDataset createDatasetDouble(ArrayList<Double> listDouble, ArrayList<String> listString) {
		DefaultPieDataset result = new DefaultPieDataset();
		for (int i = 0; i < listDouble.size(); i++) {
			result.setValue(listString.get(i), listDouble.get(i));
		}
		return result;

	}

	/**
	 * Creates a chart
	 */
	private JFreeChart createChart(PieDataset dataset, String title) {

		JFreeChart chart = ChartFactory.createPieChart3D(title, // chart title
				dataset, // data
				true, true, false);

		PiePlot3D plot = (PiePlot3D) chart.getPlot();
		plot.setStartAngle(290);
		plot.setDirection(Rotation.CLOCKWISE);
		plot.setForegroundAlpha(0.5f);
		return chart;
	}

	public static void main(String args[]) throws IOException {
		TransportClient client = null;
		try {
			client = new PreBuiltTransportClient(Settings.EMPTY)
					.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));

			Settings settings = Settings.builder().put("client.transport.sniff", true).build();
		} catch (UnknownHostException u) {
			System.out.println("Host not recognized");
		}

		// // //Indexing Data..To add more data
		// XContentBuilder builder = jsonBuilder().
		// startObject()
		// .field("city_StateProperty", "Chennai")
		// .field("name_StateProperty", "Archana")
		// .field("area_StateProperty", "Thiru")
		// .field("id_StateProperty",6875943)
		// .field("supervisor_StateProperty", "Mr. Aswin")
		// .field("supervisorId_StateProperty", 908712)
		// .field("name_TaskCategory", "Software Development")
		// .field("id_TaskCategory", 5749433)
		// .field("subcategory_TaskCategory", "Designing")
		// .field("name_TaskCategory", "Planning")
		// .field("subTaskName_TaskCategory", "SRS")
		// .field("subTaskID_TaskCategory", 6845043)
		// .field("name_ExecutiveInfo", "Mr. Wahib")
		// .field("id_ExecutiveInfo", 6832085)
		// .field("location_ExecutiveInfo", "NorthDelhi")
		// .field("timing_TaskTiming", "10AM - 11 PM")
		// .field("duration_TaskTiming", 13)
		// .field("date_TaskTiming", "2/5/2017")
		// .field("startingTime_TaskTiming", "10 AM")
		// .field("endingTime_TaskTiming", "11 PM")
		// .field("status_TaskTiming", "Complete")
		// .endObject();
		//
		//
		//
		//
		// String json = builder.string();
		// // To make the index unique we can set the index name as the user_id
		// IndexResponse resp = client.prepareIndex("servdata",
		// "data").setSource(builder).execute().actionGet();
		// System.out.println("");
		// System.out.println(json);
		// client.close();

		ArrayList<Integer> listInt = new ArrayList<Integer>();
		ArrayList<String> listString = new ArrayList<String>();
		ArrayList<Double> listDouble = new ArrayList<Double>();

		// SearchResponse for Property State City
		System.out.println("Property State City Count" + "\n");
		SearchResponse searchResponse = client.prepareSearch("servdata")
				.addAggregation(AggregationBuilders.terms("city_wise").field("city_StateProperty")).execute()
				.actionGet();

		Terms terms = searchResponse.getAggregations().get("city_wise");
		Collection<Terms.Bucket> buckets = terms.getBuckets();
		for (Bucket bucket : buckets) {
			System.out.println("City : " + bucket.getKeyAsString() + "\n" + "Count : " + bucket.getDocCount() + "\n");
			listString.add(bucket.getKeyAsString().toUpperCase());
			listInt.add((int) bucket.getDocCount());
		}

		// Displaying the chart
		ejConn = new EJConn("Data", "State Properties City Wise");
		ejConn.initialize(listInt, listString, "stateCity");
		ejConn.pack();
		ejConn.setVisible(true);
		listInt.clear();
		listString.clear();

		// Task Completion Per City (Using percentage method)
		System.out.println("Task Completion Percentage Per City" + "\n");
		SearchResponse completion_city = client.prepareSearch("servdata")
				.setQuery(QueryBuilders.matchQuery("status_TaskTiming.keyword", "Complete"))
				.addAggregation(AggregationBuilders.terms("city_wise_completion").field("city_StateProperty")).execute()
				.actionGet();
		int total_hits = (int) completion_city.getHits().getTotalHits();

		Terms terms_city = completion_city.getAggregations().get("city_wise_completion");
		Collection<Terms.Bucket> bucket_city = terms_city.getBuckets();
		for (Bucket bucket : bucket_city) {
			System.out.println("City : " + bucket.getKeyAsString());
			int result = (int) bucket.getDocCount();
			System.out.println("Percentage of Work Completed : " + (result * 100) / total_hits + "%" + "\n");
			listString.add(bucket.getKeyAsString().toUpperCase() + " = " + (result * 100) / total_hits + "%");
			listInt.add(((result * 100) / total_hits));
		}

		ejConn = new EJConn("Data", "Task Completion City Wise in Percentage");
		ejConn.initialize(listInt, listString, "taskCity");
		ejConn.pack();
		ejConn.setVisible(true);
		listInt.clear();
		listString.clear();

		// Task Completion Per Property
		System.out.println("Task Completion Percentage Per Property" + "\n");
		SearchResponse completion_propertyid = client.prepareSearch("servdata")
				.setQuery(QueryBuilders.matchQuery("status_TaskTiming.keyword", "Complete"))
				.addAggregation(AggregationBuilders.terms("property_id_completion").field("id_StateProperty")).execute()
				.actionGet();
		int total_hits_propid = (int) completion_propertyid.getHits().getTotalHits();

		Terms terms_propid = completion_propertyid.getAggregations().get("property_id_completion");
		Collection<Terms.Bucket> buckets_prop = terms_propid.getBuckets();
		for (Bucket bucket : buckets_prop) {
			System.out.println("State Property ID : " + bucket.getKeyAsString());
			int result = (int) bucket.getDocCount();
			System.out.println("Percentage Work Completed: " + (result * 100) / total_hits_propid + "%" + "\n");
			listInt.add((result * 100) / total_hits_propid);
			listString.add(bucket.getKeyAsString() + " = " + (result * 100) / total_hits_propid + "%");
		}

		ejConn = new EJConn("Data", "Task Completion Property Wise in Percentage");
		ejConn.initialize(listInt, listString, "taskProperty");
		ejConn.pack();
		ejConn.setVisible(true);
		listInt.clear();
		listString.clear();

		// Man Utilization per city
		System.out.println("Man Utilization Per City" + "\n");
		SearchResponse completion_utilizationCity = client.prepareSearch("servdata")
				.setQuery(QueryBuilders.matchAllQuery())
				.addAggregation(AggregationBuilders.terms("manutilization_city").field("city_StateProperty")
						.subAggregation(AggregationBuilders.avg("average_city").field("duration_TaskTiming")))
				.execute().actionGet();

		Terms terms_utilizationCity = completion_utilizationCity.getAggregations().get("manutilization_city");
		Collection<Terms.Bucket> buckets_utilizationCity = terms_utilizationCity.getBuckets();
		for (Bucket bucket : buckets_utilizationCity) {
			System.out.println("City Name : " + bucket.getKeyAsString());
			int result = (int) bucket.getDocCount();
			System.out.println("Count : " + result);
			double average = ((Avg) bucket.getAggregations().get("average_city")).getValue();
			System.out.println("Average Hours Worked Per Day : " + average / 8 + "\n");
			listString.add(bucket.getKeyAsString().toUpperCase());
			listDouble.add(average / 8);
		}

		ejConn = new EJConn("Data", "Average ManPower Utilization Per City");
		ejConn.initializeDouble(listDouble, listString, "manpowerCity");
		ejConn.pack();
		ejConn.setVisible(true);
		listDouble.clear();
		listString.clear();

		// Man Utilization Per Property Id
		System.out.println("Man Utilization Per Property Id" + "\n");
		SearchResponse completion_utilizationProperty = client.prepareSearch("servdata")
				.setQuery(QueryBuilders.matchAllQuery())
				.addAggregation(AggregationBuilders.terms("manutilization_property").field("id_StateProperty")
						.subAggregation(AggregationBuilders.avg("average_city").field("duration_TaskTiming")))
				.execute().actionGet();

		Terms terms_utilizationProp = completion_utilizationProperty.getAggregations().get("manutilization_property");
		Collection<Terms.Bucket> buckets_utilizationProp = terms_utilizationProp.getBuckets();
		for (Bucket bucket : buckets_utilizationProp) {
			System.out.println("State Property Id : " + bucket.getKeyAsString());
			int result = (int) bucket.getDocCount();
			System.out.println("Count : " + result);
			double average = ((Avg) bucket.getAggregations().get("average_city")).getValue();
			System.out.println("Average Hours per day : " + average / 8 + "\n");
			listString.add(bucket.getKeyAsString());
			listDouble.add(average / 8);
		}

		ejConn = new EJConn("Data", "Average ManPower Utilization Per PropertyID");
		ejConn.initializeDouble(listDouble, listString, "manpowerProperty");
		ejConn.pack();
		ejConn.setVisible(true);
		listDouble.clear();
		listString.clear();

	}
}


