package org.sourygna;

import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class AnalisisLogsDriver {

	public static void main(String[] args) throws Exception {

		//Comprobamos que los argumentos de entrada son correctos
		if (args.length != 2) {
			System.out.printf("Usage: WordCount <input dir> <output dir>\n");
			System.exit(-1);
		}
		String input = args[0];
		String output = args[1];


		Job job = Job.getInstance();
		job.setJarByClass(AnalisisLogsDriver.class);
		job.setJobName("Analisis Logs");
		
		//Definimos formato y directorio de la entrada
		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(input));

		//Seteamos la clase mapper y el tipo de clave y valor
		job.setMapperClass(LogMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(ProcessCounterWritable.class);
		
		//Seteamos la clase reducer y el tipo de clave y valor
		job.setReducerClass(LogReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		//Definimos formato y directorio de la salida
		job.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		boolean success = job.waitForCompletion(true);
		
		//Sacamos por pantalla los contadores de componentes totales
		Iterator iterCounters = job.getCounters().getGroup("Component Counters").iterator();		
		while (iterCounters.hasNext()) {
			Counter counter = (Counter) iterCounters.next();
			
			System.out.println(counter.getName() + ": " + counter.getValue());
		}
		 
		System.exit(success ? 0 : 1);
	}
}
