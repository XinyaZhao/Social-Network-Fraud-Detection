import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.collection.mutable.Map
import scala.collection.mutable
import collection.immutable.ListMap                                          
import java.io._

val input_path = "Desktop/example.txt"
val iset_out = "/Users/yunjianyang/Desktop/iSet.txt"
val jset_out = "/Users/yunjianyang/Desktop/jSet.txt"

/* get degree from a key-values pair */
def getDegree (s : Tuple2[String, Iterable[(String, String)]]) : List[String] = {
	var myList = List[String]()
	for (e <- s._2){
		val crt = "i" + e._2 + " j" + s._1 + " " + s._2.size
		myList = crt :: myList
	}
	myList.reverse
}

/* cost function for column-weighting */
def getCost(s : String) : Double = {
	val degree = s.toDouble
	1/(math.log(degree + 5) / math.log(2))
}

def flat_degree(s: String): List[(String, Double)] = {
	var l = List[(String, Double)]()
	val i = s.split(" ")(0)
	val j = s.split(" ")(1)
	val cij = s.split(" ")(2)
	l = (i, getCost(cij)) :: l
	l = (j, getCost(cij)) :: l
	l.reverse
}

/* get a scala mutable map from a RDD which stores the "i, j, degree of ij" */
def getCostMap(rdd : org.apache.spark.rdd.RDD[String]): org.apache.spark.rdd.RDD[(String,Double)] = {
	rdd.flatMap(flat_degree).reduceByKey((sum,n) => (sum+n))
}

/* Calcuate the f value of the whole set */
def getSetValue(c : org.apache.spark.rdd.RDD[(String,Double)]) : Double = {
	if(c.count != 0){
	val v = c.reduce((a,b) => (" ", (a._2 + b._2)))._2
	v/2
}
   else{
   	0.00
   }
}

/* get the vertex with minimum cost */
def getDeleted(c :  org.apache.spark.rdd.RDD[(String,Double)]) : String = {
	if(c.count != 0){
	val deleted = c.min()(new Ordering[Tuple2[String, Double]]() {
		override def compare(x: (String, Double), y: (String, Double)) : Int = 
		Ordering[Double].compare(x._2,y._2)
		})._1
	println("deleted:------------------------- " + deleted)
	deleted}
	else
	{
		" "
	}
}

/* update each line with a deleted vertex */
def update(degree : org.apache.spark.rdd.RDD[String], d : String) : org.apache.spark.rdd.RDD[String] = {
	var new_array = degree.collect;
	var tmp = new_array.to[mutable.ArrayBuffer]
	if(d.contains("j")){
		new_array = new_array.filterNot(s => s.split(" ")(1) == d)
		tmp = new_array.to[mutable.ArrayBuffer]
	}
	if(d.contains("i")){
		var update_j = List[String]()
		for(s <- new_array){
			if(s.split(" ")(0) == d){
				update_j = s.split(" ")(1) :: update_j
				tmp -= s
			}
		}

		val tmp_buffer = tmp.toArray
		// need a tmp buffert to deletee tmp
		for(j <- update_j){
			for(s <- tmp_buffer){
				if(s.split(" ")(1) == j){
					tmp -= s
					val iszero = s.split(" ")(2).toInt - 1
					if (iszero != 0){
						val new_line = s.split(" ")(0) + " " + s.split(" ")(1) + " " + (s.split(" ")(2).toInt - 1).toString
						tmp -= s
						tmp += new_line
					}
				}
			}
		}
	}
	sc.parallelize(tmp)
}

/* get graph from cost array*/
def getGraph(degree : org.apache.spark.rdd.RDD[String]) : List[String] = {
	var g = List[String]()
	for( c <- degree.collect){
		if(!g.contains(c.split(" ")(0))){
			g = c.split(" ")(0) :: g
		}
		if (!g.contains(c.split(" ")(1))){
			g = c.split(" ")(1) :: g
		}
	}
	g
}

/* iterative delete a vertex */
def greedyDecreasing(degree : org.apache.spark.rdd.RDD[String]) : List[String] = {
	val cost = getCostMap(degree)
	val value = getSetValue(cost) / cost.count
	var valueMap = Map[Int, Double]()
	valueMap += (0 -> value)
	var graph = List[List[String]]()
	graph = getGraph(degree)::graph
	var new_degree = degree
	//degree.foreach(println)
	var c = cost
	var a = 0;
	while ( c.count != 0 ){ // not cost size, need to be the number of vertex 
		println("c.count : " + c.count)
		a = a + 1;
		val d = getDeleted(c)
	    new_degree = update(new_degree, d)
		//newDegree = update(deleted) //update the degree of remaining i and j based on the deteled vertex
	    c = getCostMap(new_degree)
	    graph = getGraph(new_degree) :: graph
		val value = getSetValue(c) / c.count // the set vaule should be divided by the |C|
		println("value : " + value)
		//new_degree.foreach(println)
		//println(getGraph(c))
		valueMap += (a -> value)
	}
	var max_index = -1;
	var max_Value = -1.000;
	for(s <- valueMap){
	  if(s._2 > max_Value){
	    max_index = s._1
	    max_Value = s._2
	  }
	}
	println("maxvalue" + " " + max_Value  + " index:" + max_index)
	//graph.reverse.foreach(println)
	val objectGraph = graph.reverse(max_index)
	objectGraph
}

/* get the most density graph*/
def getFinalSet(cst : Map[String, Double], dgr : List[String]) : Set[String] ={
	var set = Set[String]()
	for( e <- cst){
		set += (e._1)
	}
	set -- dgr.toSet
}

def outputResult(set : Set[String]) : Unit = {
	val ibf = new BufferedWriter(new FileWriter(iset_out));
	val jbf = new BufferedWriter(new FileWriter(jset_out));
	val sorted_list = set.toList.sortWith(_.substring(1).toLong<_.substring(1).toLong)
	for( s <- sorted_list){
		if(s.contains("i")){
			ibf.write(s + "\n");
		}
		else{
			jbf.write(s + "\n");
		}
	}
	ibf.flush();
	jbf.flush();
	ibf.close();
	jbf.close();
}

val lines = sc.textFile(input_path);
val pairs = lines.map(x => (x.split(" ")(1), x.split(" ")(0)));
val group = pairs.groupBy(x => x._1);
var degree = group.flatMap(getDegree)
//ListMap(cost.toList.sortBy{_._2}:_*).foreach(println)
val objectGraph = greedyDecreasing(degree)
//outputResult(objectGraph.toSet)