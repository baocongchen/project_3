package project_3

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import org.apache.spark.storage.StorageLevel
import org.apache.log4j.{Level, Logger}

object main{
  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.spark-project").setLevel(Level.WARN)

  def LubyMIS(g_in: Graph[Int, Int]): Graph[Int, Int] = {
    val r = scala.util.Random;
    var remaining_vertices = 2;
    var g = g_in.mapVertices((id, vd) => (0.asInstanceOf[Int], 0.asInstanceOf[Float]));
    var loop = 1;
    while (remaining_vertices >= 1) {
        g = g.mapVertices((id, vd) => (vd._1, r.nextFloat()));
        val v = g.aggregateMessages[(Int, Float)](
                e => {
                  e.sendToDst(if ((e.srcAttr._2 + e.srcAttr._1) > (e.dstAttr._2 + e.dstAttr._1)) (0, 0) else (1, 0));
                  e.sendToSrc(if ((e.srcAttr._2 + e.srcAttr._1) > (e.dstAttr._2 + e.dstAttr._1)) (1, 0) else (0, 0))
                },
                (msg1, msg2) => if (msg1._1 == 1 && msg2._1 == 1) (1, 0) else (0, 0)
              );



        val g1 = Graph(v, g.edges);

        val v2 = g1.aggregateMessages[(Int,Float)](
            e => {
                e.sendToDst(if (e.dstAttr._1 == 1) (1, 0) else (if (e.srcAttr._1 == 1) (-1, 0) else (0, 0)));
                e.sendToSrc(if (e.srcAttr._1 == 1) (1, 0) else (if (e.dstAttr._1 == 1) (-1, 0) else (0, 0)))
              },
              (msg1, msg2) => if (msg1._1 == 1 || msg2._1 == 1) (1, 0) else (if (msg1._1 == -1 || msg2._1 == -1) (-1, 0) else (0, 0))
            )

        g = Graph(v2, g.edges);
        g.cache();
        remaining_vertices = g.vertices.filter({ case (id, x) => (x._1 == 0) }).count().toInt;
        println("Remaining vertices: " + remaining_vertices)
        loop = loop + 1;
    }
    println("Luby's algorithm completed in " + (loop-1) + " loops")
    return g.mapVertices((id, vd) => (vd._1))
  }


  def verifyMIS(g_in: Graph[Int, Int]): Boolean = {
    // To Implement
    var g = g_in;
    val vertices = g.aggregateMessages[Int]( 
    	triplet => {
    		if (triplet.srcAttr == 1 & triplet.srcAttr == triplet.dstAttr) {
    			triplet.sendToDst(1); 
    			triplet.sendToSrc(1);
    		} else {
    			triplet.sendToDst(0); 
    			triplet.sendToSrc(0);
    		}
    	},
    	(a, b) => (a + b)
    );
    //println(vertices.map(v => v._2).reduce((a,b)=>a+b));
	return (vertices.map(v => v._2).reduce((a,b)=>a+b) == 0);
  }


  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("project_3")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder.config(conf).getOrCreate()
/* You can either use sc or spark */

    if(args.length == 0) {
      println("Usage: project_3 option = {compute, verify}")
      sys.exit(1)
    }
    if(args(0)=="compute") {
      if(args.length != 3) {
        println("Usage: project_3 compute graph_path output_path")
        sys.exit(1)
      }
      val startTimeMillis = System.currentTimeMillis()
      //val edges = sc.textFile(args(1)).map(line => {val x = line.split(","); Edge(x(0).toLong, x(1).toLong , 1)} )
      val edges = sc.textFile(args(1)).map(line => {val x = line.split(","); Edge(x(0).toLong, x(1).toLong , 1)} ).filter({case Edge(a,b,c) => a!=b})
      val g = Graph.fromEdges[Int, Int](edges, 0, edgeStorageLevel = StorageLevel.MEMORY_AND_DISK, vertexStorageLevel = StorageLevel.MEMORY_AND_DISK)
      val g2 = LubyMIS(g)

      val endTimeMillis = System.currentTimeMillis()
      val durationSeconds = (endTimeMillis - startTimeMillis) / 1000
      println("==================================")
      println("Luby's algorithm completed in " + durationSeconds + "s.")
      println("==================================")

      val g2df = spark.createDataFrame(g2.vertices)
      g2df.coalesce(1).write.format("csv").mode("overwrite").save(args(2))
    }
    else if(args(0)=="verify") {
      if(args.length != 3) {
        println("Usage: project_3 verify graph_path MIS_path")
        sys.exit(1)
      }

      val edges = sc.textFile(args(1)).map(line => {val x = line.split(","); Edge(x(0).toLong, x(1).toLong , 1)} )
      val vertices = sc.textFile(args(2)).map(line => {val x = line.split(","); (x(0).toLong, x(1).toInt) })
      val g = Graph[Int, Int](vertices, edges, edgeStorageLevel = StorageLevel.MEMORY_AND_DISK, vertexStorageLevel = StorageLevel.MEMORY_AND_DISK)

      val ans = verifyMIS(g)
      if(ans)
        println("Yes")
      else
        println("No")
    }
    else
    {
        println("Usage: project_3 option = {compute, verify}")
        sys.exit(1)
    }
  }
}
