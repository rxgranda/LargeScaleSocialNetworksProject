package project


import org.apache.spark.graphx.{VertexId, _}
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import scala.runtime.ScalaRunTime._


class VertexProperty()

case class User(val places:Array[Double]) extends VertexProperty

case class Location(val lat: String, val long: String) extends VertexProperty

object Main {

  //  def parseFlight(str: String): User = {
  //    val line = str.split("\t")
  //
  //  }
  def printGraph(g:Graph[Object,String]): Unit ={
    g.vertices.map{ case (id, attr) => {
      attr match {
        case attr: Map[Long,Double]=>(id ,stringOf(attr))
        case _ => (id, attr)
      }
    }
    }.collect.foreach(println)

  }

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
//    val csv = sc.textFile("./Gowalla_edges.txt")
//    //Create User-Friendship Edges
//    val friendshipEdges = csv.map { line =>
//      val fields = line.split("\t")
//      Edge(fields(0).toLong, fields(1).toLong, "")
//    }
//    //Create User nodes
//    val users: RDD[(Long, VertexProperty)] = friendshipEdges.map(friendShip => (friendShip.srcId, User().asInstanceOf[VertexProperty])).distinct
//    //Load Second CSV
//    val csv2 = sc.textFile("./Gowalla_totalCheckins.txt")
//    //Create User-Location Edges
//    val usrLocationEdges = csv2.map { line =>
//      val fields = line.split("\t")
//      Edge(fields(0).toLong, fields(4).toLong * 1000L, fields(1))
//    }
//    //Create Location Nodes
//    val locationNodes: RDD[(Long, VertexProperty)] = csv2.map { line =>
//      val fields = line.split("\t")
//      (fields(4).toLong * 1000L, Location(fields(2), fields(3)).asInstanceOf[VertexProperty])
//    }.distinct
//Just show 5 samples
//    friendshipEdges.take(5).foreach(println)
//    users.take(5).foreach(println)
//    usrLocationEdges.take(5).foreach(println)
//    locationNodes.take(5).foreach(println)
    //Merge nodes and edges of different types
    //val nodes = users.union(locationNodes);
    //val edges = friendshipEdges.union(usrLocationEdges);

    val vertices = Array[ ( VertexId, VertexProperty) ] (
      ( 10L, Location("51.903491456","8.636259513")) ,
      ( 11L, Location("51.903491456","8.636259513")) ,
      ( 12L, Location("51.903491456","8.636259513")) ,
      ( 13L, Location("51.903491456","8.636259513")) ,
      ( 14L, Location("51.903491456","8.636259513")) ,
      ( 1L, User(Array())) ,
      ( 2L, User(Array())) ,
      ( 3L, User(Array())) ,
      ( 4L, User(Array())) ,
      ( 5L, User(Array())) ,
    )
    val nodes= sc.parallelize(vertices)

    val links = Array(
      Edge( 1L, 4L,"" ),
      Edge( 2L, 4L ,""),
      Edge( 2L, 5L,"" ),
      Edge( 3L, 5L,"" ),
      Edge( 10L, 1L,"2010-10-19T23:55:27Z" ),
      Edge( 11L, 1L,"2010-10-19T23:55:27Z" ),
      Edge( 12L, 2L,"2010-10-19T23:55:27Z" ),
      Edge( 13L, 2L,"2010-10-19T23:55:27Z" ),
      Edge( 14L, 3L,"2010-10-19T23:55:27Z" ),
      Edge( 11L, 4L,"2010-10-19T23:55:27Z" ),
      Edge( 12L, 4L,"2010-10-19T23:55:27Z" ),
      Edge( 12L, 5L,"2010-10-19T23:55:27Z" ),
      Edge( 14L, 5L,"2010-10-19T23:55:27Z" ),
      Edge( 10L, 1L,"2010-10-19T23:55:27Z" ),
    )
    val edges= sc.parallelize(links)
    //Sample 5
    nodes.take(5).foreach(println)
    edges.take(5).foreach(println)
    //Create Graph
    val graph= Graph(nodes, edges)

    //Query how many links have user #1
    println(graph.edges.filter(e => e.srcId == 1).count)

    val locations = graph.vertices.filter{
      case (id, vp: Location) => true
      case _ => false
    }

    val newVertices = graph.vertices.map { case (id, attr) => {
      attr match {
        case attr: User => (id, Map[Long,Double]())
        case _ => (id, attr)
      }

    }
    }
      val userMapsProperty=Graph(newVertices,graph.edges)
       printGraph(userMapsProperty)

//println("Mensajes")
      //    locations
    val messages = userMapsProperty.aggregateMessages[String](
      ctx =>{
        ctx.srcAttr match {
            case srcAttr : Location => {
              ctx.sendToDst(ctx.srcId+"")
            }
            case _=>null
          }
      },
      (a,b)=>a+","+b
    )
  //messages.foreach(println)
    val joined:Graph[Object,String] = userMapsProperty.outerJoinVertices(messages) {
      (id, origValue, msgValue ) => msgValue match {
        case Some(places:String) => {
          var mapa = new scala.collection.mutable.HashMap[Long, Double]
          for (i <- places.split(",")) {
            if (mapa.contains(i.toLong)) mapa(i.toLong) += 1.0
            else mapa(i.toLong) = 1.0
          }
          mapa
        }  // vertex received msg
        case None => origValue
      }
    }
    println("Grafo Agregado")
    printGraph(joined)



//    val sssp = graph.pregel(Double.PositiveInfinity)(
//      (id, dist, newDist) => dist, // Vertex Program
//      triplet => {  // Send Message
//
//        triplet.srcAttr match {
//          case srcAttr : Location => {
//            Iterator((triplet.dstId, (triplet.srcId,1)))
//          }
//          case _=>Iterator.empty
//        }
//      }
//      ,
//      ((id1,cnt), (id2,cnt2)) => {
//        if(id1==id2)
//          (a_1)
//      } // Merge Message
//    )

//    graph.collect.foreach {
//      case ( id, User(  ) ) => println( s"$id" )
//      case _ =>
//    }
    spark.stop()
  }
}

