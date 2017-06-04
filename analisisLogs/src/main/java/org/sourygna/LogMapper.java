package org.sourygna;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class LogMapper extends
		Mapper<LongWritable, Text, Text, ProcessCounterWritable> {

	private static Logger logger = Logger.getLogger(LogMapper.class);
	private HashMap<String, Integer> buffer;
	private Text outputKey = new Text();
	private ProcessCounterWritable outputValue = new ProcessCounterWritable();

	@Override
	protected void setup(
			Mapper<LongWritable, Text, Text, ProcessCounterWritable>.Context context)
			throws IOException, InterruptedException {
		// Instanciamos una tabla hash que servira de buffer del in-mapper
		// reducer
		buffer = new HashMap<String, Integer>();
	}

	@Override
	protected void map(
			LongWritable key,
			Text value,
			Mapper<LongWritable, Text, Text, ProcessCounterWritable>.Context context)
			throws IOException, InterruptedException {

		// Recuperamos los valores de la linea de log separados por comas
		String[] words = value.toString().split(" ");

		Date dFechaLog;
		SimpleDateFormat sdf;
		String sFechaLog;
		String sComponentName;
		String sBufferKey;
		Integer iBufferValue;
		
		try {
			// Recuperamos el nombre del componente
			sComponentName = words[4].split("[\\[:]")[0];

			// Si el nombre del componente contiene vmnet no se guarda
			if (!sComponentName.contains("vmnet")) {
				
				// Convertimos la fecha al formato deseado
				sdf = new SimpleDateFormat("yyyy-MMM-dd HH");
				sFechaLog = "2014-" + words[0] + "-" + words[1] + " "
						+ words[2];
				dFechaLog = sdf.parse(sFechaLog);
				sdf.applyPattern("dd/MM/yyyy-HH");
				
				//Creamos la clave de nuestro buffer del in-mapper combiner
				//La clave esta formada por fecha-hora;nombreComponente
				sBufferKey = sdf.format(dFechaLog) + ";" + sComponentName;

				// Buscamos la clave en el buffer
				iBufferValue = buffer.get(sBufferKey);

				// Si la clave no se encuentra insertamos un 1 como valor
				if (iBufferValue == null) {
					buffer.put(sBufferKey, 1);
				}
				// Si se encuentra la clave incrementamos su valor en 1
				else {
					buffer.put(sBufferKey, iBufferValue + 1);
				}
			}

		} catch (ParseException e) {
			// Si encontramos error se muestra en el log
			logger.error("ERRROR mapper: " + value.toString(), e);
			// También incrementamos el contador de errores del mapper
			context.getCounter("My Counters", "Mapper Errors").increment(1);
		} catch (Exception e) {
			// Si encontramos error se muestra en el log
			logger.error("ERRROR mapper: " + value.toString(), e);
			// También incrementamos el contador de errores del mapper
			context.getCounter("My Counters", "Mapper Errors").increment(1);
			throw e;
		}
	}

	@Override
	protected void cleanup(
			Mapper<LongWritable, Text, Text, ProcessCounterWritable>.Context context)
			throws IOException, InterruptedException {

		String[] sArrayFechaComponente;
		
		// Recorremos el buffer del in-mapper reducer
		for (Entry<String, Integer> entry : buffer.entrySet()) {
			
			//La salida del in-mapper combiner tendra la fecha-hora como clave
			sArrayFechaComponente = entry.getKey().split(";");
			outputKey.set(sArrayFechaComponente[0]);
			
			//Como valor un objeto writable que guarda nombre componente y contador
			outputValue.setProcess(sArrayFechaComponente[1]);
			outputValue.setCounter(entry.getValue());
			
			context.write(outputKey, outputValue);
		}
	}

}
