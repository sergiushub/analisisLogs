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
	private HashMap<String, HashMap<String, Integer>> buffer;
	private Text outputKey = new Text();
	private ProcessCounterWritable outputValue = new ProcessCounterWritable();

	@Override
	protected void setup(
			Mapper<LongWritable, Text, Text, ProcessCounterWritable>.Context context)
			throws IOException, InterruptedException {
		// Instanciamos una tabla hash que servira de buffer del in-mapper
		// reducer
		buffer = new HashMap<String, HashMap<String, Integer>>();
	}

	@Override
	protected void map(
			LongWritable key,
			Text value,
			Mapper<LongWritable, Text, Text, ProcessCounterWritable>.Context context)
			throws IOException, InterruptedException {

		// Recuperamos los valores de la linea de log separados por comas
		String[] words = value.toString().split(" ");

		String sFechaLog;
		String sBufferKey;
		HashMap<String, Integer> hBufferValue;
		Integer iProcessNum;
		Date dFechaLog;
		SimpleDateFormat sdf;

		try {
			// Recuperamos el nombre del componente
			String sProcessName = words[4].split("[\\[:]")[0];

			// Si el nombre del componente contiene vmnet no se guarda
			if (!sProcessName.contains("vmnet")) {
				
				// Convertimos la fecha a nuestra key
				sdf = new SimpleDateFormat("yyyy-MMM-dd HH");
				sFechaLog = "2014-" + words[0] + "-" + words[1] + " "
						+ words[2];
				dFechaLog = sdf.parse(sFechaLog);
				sdf.applyPattern("dd/MM/yyyy-HH");

				// Buscamos la clave en el buffer
				sBufferKey = sdf.format(dFechaLog);
				hBufferValue = buffer.get(sBufferKey);

				// Si la fecha no se encuentra insertamos la fecha y como valor
				// un hashtable con el proceso y un 1
				if (hBufferValue == null) {
					hBufferValue = new HashMap<String, Integer>();
					hBufferValue.put(sProcessName, 1);
					buffer.put(sBufferKey, hBufferValue);
				}
				// Si la fecha se encuentra
				else {
					// Buscamos el proceso para esa fecha
					iProcessNum = hBufferValue.get(sProcessName);

					// Si no encontramos el proceso para esa fecha insertamos
					// con un 1
					if (iProcessNum == null) {
						hBufferValue.put(sProcessName, 1);
					}
					// Si encontrmaos el proceso para esa fecha incrementamos el
					// valor en 1
					else {
						hBufferValue.put(sProcessName, iProcessNum + 1);
					}

					buffer.put(sBufferKey, hBufferValue);
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

		// Recorremos el buffer del in-mapper reducer
		for (Entry<String, HashMap<String, Integer>> entry : buffer.entrySet()) {
			// Guardamos los datos de salida del mapper
			for (Entry<String, Integer> entry2 : entry.getValue().entrySet()) {
				outputKey.set(entry.getKey());
				outputValue.setProcess(entry2.getKey());
				outputValue.setCounter(entry2.getValue());
				context.write(outputKey, outputValue);
			}
		}
	}

}
