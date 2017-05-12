import java.io._
val bf = new BufferedWriter(new FileWriter("/home/xz1863/hw/project/Input_v2.txt",true));
val lines = sc.textFile("/user/xz1863/project/gplus/Input_v1.txt");
val pairs = lines.map(x => (x.split("	")(0), x.split("	")(1)));
for (rcd <- pairs.collect){
    bf.write("i"+rcd._1 + "	" + "j"+rcd._2 + "\n")
}
bf.flush()
bf.close()