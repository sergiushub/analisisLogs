package org.sourygna;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class LogReducer extends Reducer<Text, ProcessCounterWritable, Text, Text> {

	private Text outputKey = new Text();
	private Text outputValue = new Text();
	private HashMap<String,Integer> hProcess = new HashMap<String,Integer>();

	@Override
	protected void reduce(Text key, Iterable<ProcessCounterWritable> values,
			Reducer<Text, ProcessCounterWritable, Text, Text>.Context context)
			throws IOException, InterruptedException {
		Integer iCounter;
		
		for(ProcessCounterWritable val : values) {
			//HacerOtroHashtable
			iCounter = hProcess.get(val.getProcess());
			if (iCounter == null) {
				hProcess.put(val.getProcess(), val.getCounter());
			} else {
				hProcess.put(val.getProcess(), val.getCounter() + iCounter);
			}
		}
		
		//Recorremos el buffer del in-mapper reducer
		String sOutputValue = "";
		for(Entry<String, Integer> entry: hProcess.entrySet()) {
			sOutputValue += entry.getKey();
			sOutputValue += ":";
			sOutputValue += entry.getValue() + ",";
			
			// Incrementamos el contador de componentes
			context.getCounter("Component Counters", entry.getKey()).increment(entry.getValue());
		}
		
		//Quitamos la ultima coma
		sOutputValue = sOutputValue.substring(0, sOutputValue.length() - 1);
				
		outputKey.set("[" + key.toString() + "]");
		outputValue.set(sOutputValue);
		
		context.write(outputKey, outputValue);
		
		hProcess.clear();
	}
	
}
