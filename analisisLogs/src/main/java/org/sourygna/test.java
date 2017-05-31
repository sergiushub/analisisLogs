package org.sourygna;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class test {	
	
	public static void main(String[] args) throws ParseException {
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MMM-dd HH");
		String sFechaLog = "2014-Nov-29 09:13:33";
		Date dFechaLog = sdf.parse(sFechaLog);
		sdf.applyPattern("dd/MM/yyyy-HH");
		System.out.println(sdf.format(dFechaLog));
	}
}
