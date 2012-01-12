package de.tuhh.parallel.cw.jobprocessor;
/**
 * Class that implements the Temperature Converter logic.
 * 
 * @author Román Masanés Martínez
 * @version 1.11.2011
 */
public class TemperatureConverter {
	
	/**
	 * Method that implements a Celsius to Farenheit conversor
	 * 
	 * @param cValue Temperature value expressed in Celsius
	 * @return Temperature value in Farenheit
	 */
	public double c2fConversion(double cValue){
		double fValue = (cValue*9/5+32);
		return fValue;
	}
	
	/**
	 * Method that implements a Farenheit to Celsius conversor
	 * 
	 * @param fValue Temperature value expressed in Farenheit
	 * @return Temperature value in Celsius
	 */
	public double f2cConversion(double fValue){
		double cValue = (fValue-32)*5/9;
		return cValue;
	}
}
