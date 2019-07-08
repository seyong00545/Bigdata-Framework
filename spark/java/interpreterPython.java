
package jythonTest;

import org.python.util.PythonInterpreter;
import java.io.*;
import java.util.*;
import java.util.logging.*;

public class interpreterPython{
	public static void main(String[] args){
		System.setProperty("python.cachedir.skip", "true");
		PythonInterpreter interpreter = new PythonInterpreter();
		interpreter.execfile("/home/ice/spark/examples/src/main/python/ml/als_edge.py");
		interpreter.exec("print(recommendProduct([Row(product_code=4107)]))");
	}
}
