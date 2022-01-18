package tn.isima.tp2;
 import org.apache.hadoop.io.DoubleWritable;
 import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
 import java.io.IOException;
 import java.util.StringTokenizer;
 public class TokenizerMapper
 extends Mapper<Object, Text, Text, DoubleWritable>{
 
 public static boolean isNumeric(String string) {
 Double intValue;
 System.out.println(String.format("Parsing string: \"%s\"", string));
 if(string == null || string.equals("")) {
 System.out.println("String cannot be parsed, it is null or empty.");
 return false;
 }
 
 try {
 intValue = Double.parseDouble(string);
 return true;
 } catch (NumberFormatException e) {
 System.out.println("Input String cannot be parsed to Integer.");
 }
 return false;
}
 
 private Text word = new Text();
 public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {
 String 
s1=value.toString().toString().trim().replaceAll(",", "\t");
 
 System.out.println("s1="+s1);
 StringTokenizer itr = new StringTokenizer(s1);
 String author="";
 String content="";
 String date_time="";
 Double id=0.0;
 String language="";
 String latitude="";
 String longitude="";
 Integer number_of_likes=0;
 Integer number_of_hsares=0;
 
 int i=0;
while (itr.hasMoreTokens()) {
 v=itr.nextToken();
    if(i==1) {author=v.toString();}
    if(i==8){number_of_likes=Integer.parseInt(v.toString());}
    if(i==9){number_of_hsares=Integer.parseInt(v.toString());}
 
 context.write(new Text(author), new IntWritable(number_of_likes), new IntWritable(number_of_hsares));
 }
 
System.out.println("author="+author+"number of likes"+number_of_likes+"number of shares"+number_of_hsares);
 i++;
 
 
 }
 
 
}