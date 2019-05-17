package project


import org.apache.spark.graphx._
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD


class VertexProperty()

case class User() extends VertexProperty

case class Location(val lat: String, val long: String) extends VertexProperty

object Main {

  //  def parseFlight(str: String): User = {
  //    val line = str.split("\t")
  //
  //  }

  def main(args: Array[String]): Unit = {
    // Creates a SparkSession.
    val spark = SparkSession
      .builder
      .appName("Project")
      .config("spark.master", "local")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val sc = spark.sparkContext
    //Load First CSV
    val csv = sc.textFile("./Gowalla_edges.txt")
    //Create User-Friendship Edges
    val friendshipEdges = csv.map { line =>
      val fields = line.split("\t")
      Edge(fields(0).toLong, fields(1).toLong, "")
    }
    //Create User nodes
    val users: RDD[(Long, VertexProperty)] = friendshipEdges.map(friendShip => (friendShip.srcId, User().asInstanceOf[VertexProperty])).distinct
    //Load Second CSV
    val csv2 = sc.textFile("./Gowalla_totalCheckins.txt")
    //Create User-Location Edges
    val usrLocationEdges = csv2.map { line =>
      val fields = line.split("\t")
      Edge(fields(0).toLong, fields(4).toLong * 1000L, fields(1))
    }
    //Create Location Nodes
    val locationNodes: RDD[(Long, VertexProperty)] = csv2.map { line =>
      val fields = line.split("\t")
      (fields(4).toLong * 1000L, Location(fields(2), fields(3)).asInstanceOf[VertexProperty])
    }.distinct
    //Just show 5 samples
//    friendshipEdges.take(5).foreach(println)
//    users.take(5).foreach(println)
//    usrLocationEdges.take(5).foreach(println)
//    locationNodes.take(5).foreach(println)
    //Merge nodes and edges of different types
    val nodes = users.union(locationNodes);
    val edges = friendshipEdges.union(usrLocationEdges);
    //Sample 5
    nodes.take(5).foreach(println)
    edges.take(5).foreach(println)
    //Create Graph
    val graph = Graph(nodes, edges)
    //Query how many links have user #1
    print(graph.edges.filter(e => e.srcId == 1).count)

    spark.stop()
  }
}

