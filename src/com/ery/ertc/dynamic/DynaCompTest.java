package com.ery.ertc.dynamic;

import com.ery.ertc.collect.forwards.send.DynaManager;
import com.ery.ertc.collect.forwards.send.ISendPlugin;

public class DynaCompTest {
	public static void main(String[] args) throws Exception {
		String fullName = "DynaClass";
		StringBuilder src = new StringBuilder();
		src.append("package com.ery.ertc.hadoop.mrddx.remote.plugin;\n");
		src.append("import java.io.IOException;\n");
		src.append("import org.apache.hadoop.mapred.JobConf;\n");
		src.append("import java.util.Map;\n");
		src.append("import com.ery.ertc.hadoop.mrddx.remote.plugin.IRemotePlugin;\n");
		src.append("public class DynaClass implements IRemotePlugin {\n");
		src.append("@Override\n");
		src.append("public String line(String lineValue) throws IOException {\n");
		src.append("return null;\n");
		src.append("}\n");
		src.append("@Override\n");
		src.append("public boolean recode(Map<String, Object> lineValue) throws IOException {\n");
		src.append("return false;\n");
		src.append("}\n");
		src.append("@Override\n");
		src.append("public void configure(JobConf job) {\n");
		src.append("\n");
		src.append("}\n");
		src.append("@Override\n");
		src.append("public void close() throws IOException {\n");
		src.append("System.out.println(\"Hello, I am1111\");");
		src.append("}\n");
		src.append("public String toString() {\n");
		src.append("return \"Hello, I am \" + this.getClass().getSimpleName();\n");
		src.append("}\n");
		src.append("}\n");

		System.out.println(src);
		ISendPlugin instance = DynaManager.INSTANCE.getPluginObject(fullName, src.toString());
		instance.close();
	}
}