import scala.collection.mutable.Map
import scala.collection.mutable
import collection.immutable.ListMap                                          
import java.io._

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

/* get a scala mutable map from a RDD which stores the "i, j, degree of ij" */
def getCostMap(rdd : org.apache.spark.rdd.RDD[String]) : Map[String, Double] = {
	var l1 = Map[String, Double]()
	for(crt <- rdd.collect){
		val i = crt.split(" ")(0)
		val cij = getCost(crt.split(" ")(2))
		if(l1.contains(i)){
			l1(i) += cij
		} else {
			l1 += (i -> cij)
		}
	}
	l1
}

/* Calcuate the f value of the whole set */
def getSetValue(c :  Map[String, Double]) : Double = {
	var sv = 0.0
	for(v <- c){
		sv += v._2
	}
	sv
}

/* get the vertex with minimum cost */
def getDeleted(c :  Map[String, Double]) : String = {
	val deleted = c.minBy(_._2)._1
	println("deleted:------------------------- " + deleted)
	deleted
}

/* update each line with a deleted vertex */
def update(degree : org.apache.spark.rdd.RDD[String], d : String) : org.apache.spark.rdd.RDD[String] = {
	var new_array = degree.collect;
	var tmp = new_array.to[mutable.ArrayBuffer]
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
	var valueMap = Map[Int, Double]()
	var graph = List[List[String]]()
	val value = getSetValue(cost) / cost.size
	valueMap += (0 -> value)
	graph = getGraph(degree)::graph
	var new_degree = degree
	//degree.foreach(println)
	var c = cost
	var a = 0;
	while ( c.size != 0 ){ // not cost size, need to be the number of vertex 
		println("c.size : " + c.size)
		a = a + 1;
		val d = getDeleted(c)
	    new_degree = update(new_degree, d)
		//newDegree = update(deleted) //update the degree of remaining i and j based on the deteled vertex
	    c = getCostMap(new_degree)
	    graph = getGraph(new_degree) :: graph
		val value = getSetValue(c) / c.size // the set vaule should be divided by the |C|
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
	val ibf = new BufferedWriter(new FileWriter("/Users/yunjianyang/Desktop/iSet.txt"));
	val jbf = new BufferedWriter(new FileWriter("/Users/yunjianyang/Desktop/jSet.txt"));
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

val lines = sc.textFile("Desktop/example.txt");
val pairs = lines.map(x => (x.split(" ")(1), x.split(" ")(0)));
val group = pairs.groupBy(x => x._1);
var degree = group.flatMap(getDegree)
val cost = getCostMap(degree)
//ListMap(cost.toList.sortBy{_._2}:_*).foreach(println)
val objectGraph = greedyDecreasing(degree)
outputResult(objectGraph.toSet)