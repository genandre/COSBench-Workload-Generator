import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
 
import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
 
public class XMLGenerator {
 
	private Document doc;
	private String num_of_workers;
	private String runtime;
	private String output_path;
	private String delay;
	private String auth_url;
	private String rampup;
	private String num_of_drivers;
	
	public static void main(String argv[]) {
 
		XMLGenerator xGen = new XMLGenerator();
	  
		BufferedReader br = null;
		
		Properties prop = new Properties();
		try {
			prop.load(new FileInputStream("config.properties"));
			xGen.num_of_workers=prop.getProperty("num_of_workers_per_stage");
			xGen.runtime=prop.getProperty("runtime");
			xGen.output_path=prop.getProperty("output_path");
			xGen.delay=prop.getProperty("delay_between_main_stages");
			xGen.auth_url=prop.getProperty("auth_url");
			xGen.rampup=prop.getProperty("rampup");
			xGen.num_of_drivers=prop.getProperty("num_of_drivers");
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
			return;
		} catch (IOException e1) {
			e1.printStackTrace();
			return;
		}
		
		try {
 
			String sCurrentLine;
 
			br = new BufferedReader(new FileReader("metrics.txt"));
			
			while ((sCurrentLine = br.readLine()) != null) {
				
				String fields[] = sCurrentLine.split("\\t");
				String sizes[];
				int sizelength;
				boolean isRange;
				String unit="KB";
				
				if(fields[1].contains("-"))
					{
						sizes = fields[1].substring(fields[1].indexOf("(")+1, fields[1].indexOf(")")).split("-");
						unit = fields[1].substring(fields[1].indexOf(")")+1);
						isRange = true;
					}
				else 
					{
						sizes = fields[1].substring(fields[1].indexOf("(")+1, fields[1].indexOf(")")).split(",");
						isRange = false;
					}
				String containers[] = fields[3].split(",");
				String readWriteDeletePercent[]= {"5,90,5","45,50,5","85,10,5","98,1,1","64,32,4"};
				String objects = fields[2];
				
				
				//isRange then set sized to just 1
				if(isRange)
					sizelength = 1;
				else
					sizelength = sizes.length;
				
				xGen.createXML(fields,sizes,containers,objects,readWriteDeletePercent,isRange,sizelength,unit);
			}
 
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (br != null)br.close();
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}
		
	}
	
	public void createXML(String[] fields, String[] sizes, String[] containers, String objects, String[] readWriteDeletePercent, boolean isRange, int sizelength, String unit)
	{
		try {
			 
			DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder docBuilder = docFactory.newDocumentBuilder();
			doc = docBuilder.newDocument();
		
			//workload element
			Element workload = doc.createElement("workload");
			doc.appendChild(workload);
			
			//workload attributes
			Attr attr = doc.createAttribute("name");
			attr.setValue(fields[0]);
			workload.setAttributeNode(attr);
			
			attr = doc.createAttribute("description");
			attr.setValue(fields[1]);
			workload.setAttributeNode(attr);
			
			//storage element
			Element storage = doc.createElement("storage");
			workload.appendChild(storage);
			
			//storage attributes
			attr = doc.createAttribute("type");
			attr.setValue("swift");
			storage.setAttributeNode(attr);
			
			//auth element
			Element auth = doc.createElement("auth");
			workload.appendChild(auth);
			
			//auth attributes
			attr = doc.createAttribute("type");
			attr.setValue("swauth");
			auth.setAttributeNode(attr);
			
			attr =doc.createAttribute("config");
			attr.setValue(auth_url);
			auth.setAttributeNode(attr);
			
			//workflow element
			Element workflow = doc.createElement("workflow");
			workload.appendChild(workflow);
			
			//multiple init stages
			int previousContainerValue=0;
			for(int i=0;i<sizelength;i++)
			{
				for(int j=0;j<containers.length;j++)
				{
					String containerString = containers[j];
					String from_container = previousContainerValue + Integer.valueOf(containerString) +"";
					String to_container	= Integer.valueOf(from_container) +  Integer.valueOf(containerString)-1 + "";
					previousContainerValue = Integer.valueOf(to_container);
					createWorkstage(workflow,"init",num_of_drivers,"containers=r("+from_container+","+to_container+")");
				}
			}
			
			//multiple prepare stages
			previousContainerValue=0;
			int previousObjectValue=0;
			for(int i=0;i<sizelength;i++)
			{
				for(int j=0;j<containers.length;j++)
				{
					String sizeString;
					if(isRange)
						sizeString = "("+sizes[0]+","+sizes[1]+")"+unit;
					else
						sizeString = "("+sizes[i].substring(0,sizes[i].length()-2)+")"+sizes[i].substring(sizes[i].length()-2);
					
					String containerString = containers[j];
					String from_container = previousContainerValue + Integer.valueOf(containerString) +"";
					String to_container	= Integer.valueOf(from_container) +  Integer.valueOf(containerString)-1 + "";
					previousContainerValue = Integer.valueOf(to_container);

					
					String from_object = previousObjectValue + 1 + "";
					String to_object	= Integer.valueOf(from_object) +  Integer.valueOf(objects)-1 + "";
					previousObjectValue = Integer.valueOf(to_object);
					
					if(isRange)
						createWorkstage(workflow,"prepare",Integer.valueOf(objects)/Integer.valueOf(num_of_drivers)+"","containers=r("+from_container+","+to_container+");objects=r("+from_object+","+to_object+");sizes=u"+sizeString);
					else
						createWorkstage(workflow,"prepare",Integer.valueOf(objects)/Integer.valueOf(num_of_drivers)+"","containers=r("+from_container+","+to_container+");objects=r("+from_object+","+to_object+");sizes=c"+sizeString);
					
				}
			}
			
			
			
			//multiple main stages
			previousContainerValue=0;
			previousObjectValue=0;
			for(int i=0;i<sizelength;i++)
			{
				for(int j=0;j<containers.length;j++)
				{
					String sizeString;
					if(isRange)
						sizeString = "("+sizes[0]+","+sizes[1]+")"+unit;
					else
						sizeString = "("+sizes[i].substring(0,sizes[i].length()-2)+")"+sizes[i].substring(sizes[i].length()-2);
					
					String containerString = containers[j];
					String from_container = previousContainerValue + Integer.valueOf(containerString) +"";
					String to_container	= Integer.valueOf(from_container) +  Integer.valueOf(containerString)-1 + "";
					previousContainerValue = Integer.valueOf(to_container);

					
					String from_object = previousObjectValue + 1 + "";
					String to_object	= Integer.valueOf(from_object) +  Integer.valueOf(objects)-1 + "";
					previousObjectValue = Integer.valueOf(to_object);
					
					for(int k=0;k<readWriteDeletePercent.length;k++)
					{
						if((k+4)>fields.length-1 || fields[k+4].length()==0)
						{
							continue;
						}
						
						String readRatio = readWriteDeletePercent[k].split(",")[0];
						String writeRatio = readWriteDeletePercent[k].split(",")[1];
						String deleteRatio = readWriteDeletePercent[k].split(",")[2];
						
						createNormalWorkstage(fields[1].substring(0, fields[1].indexOf("(")),workflow,num_of_workers,runtime,readRatio,writeRatio,deleteRatio,isRange,from_container,to_container,from_object,to_object,sizeString);
					}
					
				}
			}
			
			//multiple cleanup stages
			previousContainerValue=0;
			previousObjectValue=0;
			for(int i=0;i<sizelength;i++)
			{
				for(int j=0;j<containers.length;j++)
				{
					String containerString = containers[j];
					String from_container = previousContainerValue + Integer.valueOf(containerString) +"";
					String to_container	= Integer.valueOf(from_container) +  Integer.valueOf(containerString)-1 + "";
					previousContainerValue = Integer.valueOf(to_container);

					
					String from_object = previousObjectValue + 1 + "";
					String to_object	= Integer.valueOf(from_object) +  Integer.valueOf(objects)-1 + "";
					previousObjectValue = Integer.valueOf(to_object);
					
					createWorkstage(workflow,"cleanup","1","containers=r("+from_container+","+to_container+");objects=r("+from_object+","+to_object+")");
					
				}
			}
			
			//multiple dispose stages
			previousContainerValue=0;
			for(int i=0;i<sizelength;i++)
			{
				for(int j=0;j<containers.length;j++)
				{
					String containerString = containers[j];
					String from_container = previousContainerValue + Integer.valueOf(containerString) +"";
					String to_container	= Integer.valueOf(from_container) +  Integer.valueOf(containerString)-1 + "";
					previousContainerValue = Integer.valueOf(to_container);
					createWorkstage(workflow,"dispose","1","containers=r("+from_container+","+to_container+")");
				}
			}
			
			// write the content into xml file
			
			TransformerFactory transformerFactory = TransformerFactory.newInstance();
			Transformer transformer = transformerFactory.newTransformer();
			transformer.setOutputProperty(OutputKeys.INDENT, "yes");
			transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "10");
			DOMSource source = new DOMSource(doc);
			//StreamResult result = new StreamResult(new File(output_path+fields[0].replaceAll("\\s+", "")+"-"+fields[1].substring(0,fields[1].indexOf("(")).replaceAll("\\s+", "")+"-"+sizeString.replaceAll("[()]", "")+"-"+objectString+"-"+containerString+"-template"+".xml"));
			StreamResult result = new StreamResult(new File(output_path+fields[0].replaceAll("\\s+", "") +".xml"));
			
			
			//Output to console for testing
			//StreamResult result = new StreamResult(System.out);
	 
			transformer.transform(source, result);
	 
	 
		  } catch (ParserConfigurationException pce) {
			pce.printStackTrace();
		  } 
		catch (TransformerException tfe) {
			tfe.printStackTrace();
		  }
		
		
	}
	
	
	private void createNormalWorkstage(String workloadType,Element workflow, String workers, String runtime, String readRatio,
			String writeRatio, String deleteRatio, boolean isRange, String from_container, String to_container, String from_object, String to_object, String sizeString) 
	{
		Attr attr;
		
		Element workstage = doc.createElement("workstage");
		workflow.appendChild(workstage);
		
		String stageName = "w"+workloadType.replaceAll("\\s+", "")+sizeString+"_c"+(Integer.valueOf(to_container)-Integer.valueOf(from_container)+1)+"_o"+(Integer.valueOf(to_object)-Integer.valueOf(from_object)+1)+"_r"+readRatio+"w"+writeRatio+"d"+deleteRatio+"_NUMWORKERS";
		
		attr = doc.createAttribute("name");
		attr.setValue(stageName);
		workstage.setAttributeNode(attr);
		
		attr = doc.createAttribute("closuredelay");
		attr.setValue(delay);
		workstage.setAttributeNode(attr);
		
		Element work = doc.createElement("work");
		workstage.appendChild(work);
		
		attr = doc.createAttribute("name");
		attr.setValue(stageName);
		work.setAttributeNode(attr);
		
		attr = doc.createAttribute("workers");
		attr.setValue(workers);
		work.setAttributeNode(attr);

		attr = doc.createAttribute("rampup");
		attr.setValue(rampup);
		work.setAttributeNode(attr);
		
		attr = doc.createAttribute("runtime");
		attr.setValue(runtime);
		work.setAttributeNode(attr);
		
		//Read operation
		Element operation = doc.createElement("operation");
		work.appendChild(operation);
		
		attr = doc.createAttribute("type");
		attr.setValue("read");
		operation.setAttributeNode(attr);

		attr = doc.createAttribute("ratio");
		attr.setValue(readRatio);
		operation.setAttributeNode(attr);
		
		attr = doc.createAttribute("config");
		attr.setValue("containers=u("+from_container+","+to_container+");objects=u("+from_object+","+to_object+")");
		operation.setAttributeNode(attr);

		//Write operation
		operation = doc.createElement("operation");
		work.appendChild(operation);
		
		attr = doc.createAttribute("type");
		attr.setValue("write");
		operation.setAttributeNode(attr);

		attr = doc.createAttribute("ratio");
		attr.setValue(writeRatio);
		operation.setAttributeNode(attr);
		
		if(isRange)
		{
			attr = doc.createAttribute("config");
			attr.setValue("containers=u("+from_container+","+to_container+");objects=u("+from_object+","+to_object+");sizes=u"+sizeString);
			operation.setAttributeNode(attr);
		}
		else
		{
			attr = doc.createAttribute("config");
			attr.setValue("containers=u("+from_container+","+to_container+");objects=u("+from_object+","+to_object+");sizes=c"+sizeString);
			operation.setAttributeNode(attr);
		}
		//Delete operation
		operation = doc.createElement("operation");
		work.appendChild(operation);
		
		attr = doc.createAttribute("type");
		attr.setValue("delete");
		operation.setAttributeNode(attr);

		attr = doc.createAttribute("ratio");
		attr.setValue(deleteRatio);
		operation.setAttributeNode(attr);
		
		attr = doc.createAttribute("config");
		attr.setValue("containers=u("+from_container+","+to_container+");objects=u("+from_object+","+to_object+")");
		operation.setAttributeNode(attr);

	}

	public void createWorkstage(Element workflow, String name, String workers, String config)
	{
		Attr attr;
		
		Element workstage = doc.createElement("workstage");
		workflow.appendChild(workstage);
		
		attr = doc.createAttribute("name");
		attr.setValue(name);
		workstage.setAttributeNode(attr);
		
		Element work = doc.createElement("work");
		workstage.appendChild(work);
		
		attr = doc.createAttribute("type");
		attr.setValue(name);
		work.setAttributeNode(attr);
		
		attr = doc.createAttribute("workers");
		if(Integer.valueOf(workers)>4096)
			workers="4096";
		attr.setValue(workers);
		work.setAttributeNode(attr);
		
		attr = doc.createAttribute("config");
		attr.setValue(config);
		work.setAttributeNode(attr);
		
		
	}
}
