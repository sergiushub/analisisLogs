package org.sourygna;

import java.io.IOException;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

public class LogReducer extends
		Reducer<Text, ProcessCounterWritable, Text, Text> {

	private static Logger logger = Logger.getLogger(LogReducer.class);
	private Text outputKey = new Text();
	private Text outputValue = new Text();
	private HashMap<String, Integer> hProcess = new HashMap<String, Integer>();

	@Override
	protected void reduce(Text key, Iterable<ProcessCounterWritable> values,
			Reducer<Text, ProcessCounterWritable, Text, Text>.Context context)
			throws IOException, InterruptedException {
		Integer iCounter;

		try {

			//Recorremos los valores y creamos un hashmap donde guardaremos
			//sumaremos los contadores de cada componente
			for (ProcessCounterWritable val : values) {
				iCounter = hProcess.get(val.getProcess());
				if (iCounter == null) {
					hProcess.put(val.getProcess(), val.getCounter());
				} else {
					hProcess.put(val.getProcess(), val.getCounter() + iCounter);
				}
			}

			// Recorremos el buffer del in-mapper reducer
			String sOutputValue = "";
			for (Entry<String, Integer> entry : hProcess.entrySet()) {
				sOutputValue += entry.getKey();
				sOutputValue += ":";
				sOutputValue += entry.getValue() + ",";

				// Incrementamos el contador de componentes
				context.getCounter("Component Counters", entry.getKey())
						.increment(entry.getValue());
			}

			// Quitamos la ultima coma
			sOutputValue = sOutputValue.substring(0, sOutputValue.length() - 1);

			//Escribiomos la salida en context
			outputKey.set("[" + key.toString() + "]");
			outputValue.set(sOutputValue);
			context.write(outputKey, outputValue);

			//Limpiamos el hashmap antes de la proxima ejecucion
			hProcess.clear();

		} catch (Exception e) {
			// Si encontramos error se muestra en el log
			logger.error("ERRROR reducer: " + key.toString(), e);
			// También incrementamos el contador de errores del mapper
			context.getCounter("My Counters", "Reducer Errors").increment(1);
			throw e;
		}
	}

}
