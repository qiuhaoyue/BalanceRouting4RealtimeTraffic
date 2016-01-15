import java.io.BufferedReader;

import java.io.ByteArrayInputStream;
import java.io.IOException;
//import java.io.InputStream;
import java.io.InputStreamReader;
//import java.net.HttpURLConnection;
import java.net.MalformedURLException;
//import java.net.ProtocolException;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
//import android.util.Log;
//import com.sun.org.apache.xml.internal.security.utils.Base64;


public class Crawler {

	//private static String xml="";

	public static int getTravelTime(double x1, double y1, double x2, double y2, ArrayList<Integer> result) {
		
		//668 Fulton St, San Francisco, CA to  2253 Fulton St, San Francisco,
		String mapquest= "http://www.mapquestapi.com/directions/v1/route?key=Fmjtd%7Cluub20uznd%2C75%3Do5-9urnd6&callback=renderAdvancedNarrative&ambiguities=ignore&avoidTimedConditions=false&doReverseGeocode=true&outFormat=xml&routeType=fastest&timeType=1&enhancedNarrative=false&shapeFormat=raw&generalize=0&locale=en_US&unit=m&" +
				"from=" + y1 + "," + x1 + "&to=" + y2 + "," + x2 + "&drivingStyle=2&highwayEfficiency=21.0";
			
		System.out.println("Traffic data crawling begin!");
		URL quest = null;
		
		try{
			quest = new URL(mapquest);
		}
		catch (MalformedURLException e1) {
			e1.printStackTrace();
			return -1;
		}	
		System.out.println(quest.toString());
				
		URLConnection con = null;
		try {
			con = quest.openConnection();
		} catch (IOException e1) {
			e1.printStackTrace();
			return -2;
		}	
		System.out.println("connection fine!");
				
		BufferedReader in = null;
		try {
			in = new BufferedReader(new InputStreamReader(con.getInputStream()));
		} catch (IOException e1) {
			e1.printStackTrace();
			return -3;
		}	
		System.out.println("bufferReader OK!");
				
		String inputLine;
		String xml = "";
		try {
			while ((inputLine = in.readLine()) != null){ 
				xml = xml+inputLine;
				//System.out.println(inputLine);
			}
		} catch (IOException e1) {
			e1.printStackTrace();
			return -4;
		}
				
		try {
			in.close();
		} catch (IOException e1) {
			e1.printStackTrace();
			return -5;
		}		
		System.out.println("XML results:"+xml);
				
		XPathFactory factory = XPathFactory.newInstance();
		XPath xpath = factory.newXPath();
		NodeList nldirection =null;
		String realtime=null;
		String time=null;
		InputSource inputxml;
		try{
			inputxml= new InputSource(new ByteArrayInputStream(xml.getBytes()));
			nldirection = (NodeList) xpath.evaluate("/response/route/realTime", inputxml, XPathConstants.NODESET);
			Node node = nldirection.item(0);
			realtime= node.getTextContent();

			inputxml= new InputSource(new ByteArrayInputStream(xml.getBytes()));
			nldirection = (NodeList) xpath.evaluate("/response/route/time", inputxml, XPathConstants.NODESET);
			Node node1 = nldirection.item(0);
			time= node1.getTextContent();
		}
		catch (XPathExpressionException e) {
			e.printStackTrace();
			return -6;
		}
		
		try{
			result.add(0, Integer.parseInt(realtime));
			result.add(1, Integer.parseInt(time));
			System.out.println("realtime="+realtime+" time="+time);
		}
		catch(Exception e){
			return -7;
		}
		return 0;
	} 

}