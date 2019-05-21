package project


import org.apache.spark.graphx.{VertexId, _}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.HashMap
import org.apache.spark.rdd.RDD

import scala.collection.immutable.ListMap
import scala.runtime.ScalaRunTime._
import scala.util.parsing.json.JSONObject
import scala.collection.mutable.ArrayBuffer




class VertexProperty()

case class User(val places:Array[Double]) extends VertexProperty
case class UserPreferences(var preferences:HashMap[Long, Double],var friendsPreferences:HashMap[Long,HashMap[Long, Double] ]) extends VertexProperty
case class UserCluster(var friendsPreferences:HashMap[Long,HashMap[Long, Double] ]) extends VertexProperty

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

        case attr: UserPreferences=>(id ,"Self-Preference==> "+stringOf(attr.preferences)+"  --Neighbours-Preferences==> "++stringOf(attr.friendsPreferences))
        case _ => (id, attr)
      }
    }
    }.collect.foreach(println)

  }


  def makeRecomendations(userWithFriendPreferences:Graph[Object,String]): RDD[(VertexId,Object)] ={
    val recomendations = userWithFriendPreferences.vertices.map { case (id, attr) => {
      attr match {
        case attr: UserPreferences => {
          val friendHistory=attr.friendsPreferences
          val selfHistory=attr.preferences
          var unvisitedLocations = ArrayBuffer[Long]()

          var magnitudeU=0.0
          selfHistory foreach {case (locationID, value) =>magnitudeU+=value*value}
          magnitudeU=math.sqrt(magnitudeU)
          println(id+":  Umag:"+magnitudeU)
          var similarities=HashMap[Long,Double]()

          friendHistory foreach {case (frienID, mapPlaces) =>
            mapPlaces foreach {
              case (locationID, preference) =>{
                if (!selfHistory.contains(locationID)){
                  unvisitedLocations+= locationID
                  if(!similarities.contains(frienID)){
                    var denominator=0.0
                    mapPlaces foreach {case (locationID, value) =>denominator+=value*value}
                    denominator=math.sqrt(denominator)
                    val allPlaces=mapPlaces.keySet++selfHistory.keySet
                    var numerator=0.0
                    allPlaces.foreach((i: Long) => {
                      if(mapPlaces.contains(i)&&selfHistory.contains(i))
                        numerator+=mapPlaces(i)*selfHistory(i)
                    }
                    )
                    if(denominator!=0){
                      println(numerator)
                      numerator=numerator/(denominator*magnitudeU)
                    }else{
                      numerator=0
                    }
                    similarities(frienID)=numerator
                  }
                }
              }
            }
          }
          var scores=HashMap[Long,Double]();
          var denominator=0.0
          similarities foreach {case (frienID, value) =>{
            println(id+":    Friend: "+frienID+ "   sim:"+value)
            denominator+=value}}
          for ( loc <- unvisitedLocations ) {
            var numerator=0.0
            friendHistory foreach {case (frienID, mapPlaces) =>{
              if(mapPlaces.contains(loc))
                numerator+=similarities(frienID)*mapPlaces(loc)
            }}
            scores(loc)=numerator/denominator
          }

          println("----"+id+"   denominator: "+denominator)
          //Sort scores
          val sortedScores=ListMap(scores.toSeq.sortWith(_._2 > _._2):_*)
          (id,sortedScores)
        }
        case _ => (id, attr)
      }
    }
    }
    recomendations
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
      Edge( 13L, 3L,"2010-10-19T23:55:27Z" ),
    )
    val edges= sc.parallelize(links)
    //Sample 5
    nodes.take(5).foreach(println)
    edges.take(5).foreach(println)
    //Create Graph
    val graph= Graph(nodes, edges)

    //Query how many links have user #1
    println(graph.edges.filter(e => e.srcId == 1).count)

    //    val locations = graph.vertices.filter{
    //      case (id, vp: Location) => true
    //      case _ => false
    //    }

    val newVertices = graph.vertices.map { case (id, attr) => {
      attr match {
        case attr: User => (id, new scala.collection.mutable.HashMap[Long, Double])
        case _ => (id, attr)
      }
    }
    }
    val userMapsProperty=Graph(newVertices,graph.edges)
    printGraph(userMapsProperty)

    //println("Mensajes")
    //    locations
    //    val messages = userMapsProperty.aggregateMessages[String](
    //      ctx =>{
    //        ctx.srcAttr match {
    //          case srcAttr : Location => {
    //            ctx.sendToDst(ctx.srcId+"")
    //          }
    //          case _=>null
    //        }
    //      },
    //      (a,b)=>a+","+b
    //    )
    //    //messages.foreach(println)
    //    val userPlacesInfo:Graph[Object,String] = userMapsProperty.outerJoinVertices(messages) {
    //      (id, origValue, msgValue ) => msgValue match {
    //        case Some(places:String) => {
    //          var mapa = new scala.collection.mutable.HashMap[Long, Double]
    //          for (i <- places.split(",")) {
    //            if (mapa.contains(i.toLong)) mapa(i.toLong) += 1.0
    //            else mapa(i.toLong) = 1.0
    //          }
    //          mapa
    //        }  // vertex received msg
    //        case None => origValue
    //      }
    //    }
    //    println("Grafo Agregado")
    //    printGraph(userPlacesInfo)


    def setMsg(vertexId: VertexId, value: Object, message: String) = {
      message match {
        case message:String => {
          //print(message)
          if (message!="") {
            //var mapa = new scala.collection.mutable.HashMap[Long, Double]
            var mapa =value.asInstanceOf[scala.collection.mutable.HashMap[Long, Double]]
            for (i <- message.split(",")) {
              if (mapa.contains(i.toLong)) mapa(i.toLong) += 1.0
              else mapa(i.toLong) = 1.0
            }
            mapa
          }else{
            value
          }
        }
        case _ => value
      }
    }
    def sendMsg(triplet: EdgeTriplet[Object,String] ): Iterator[(VertexId,String)] = {
      triplet.srcAttr match {
        case srcAttr : Location => {
          Iterator((triplet.dstId,triplet.srcId+""))
        }
        case _=>Iterator.empty
      }
    }

    def mergeMsg(msg1: (String), msg2: (String)): String={
      msg1+","+msg2
    }
    val userMapsPropertyAccum : Graph[Object, String]= userMapsProperty.pregel("",maxIterations = 1)(
      setMsg, // Vertex Program
      sendMsg,// vertex received msg
      mergeMsg// Merge Message
    )
    val userMapsPropertyNormalized = userMapsPropertyAccum.vertices.map { case (id, attr) => {
      attr match {
        case attr: scala.collection.mutable.HashMap[Long,Double] => {
          var total=0.0
          //var mapa = new scala.collection.mutable.HashMap[Long, Double]
          attr foreach {case (key, value) => total+= value}
          attr foreach {case (key, value) => attr(key)=value/total}
          (id,attr )
        }
        case _ => (id,attr )
      }
    }
    }
    val newGraph=Graph(userMapsPropertyNormalized,userMapsPropertyAccum.edges)
    //println("Valores Normalizados")
    //printGraph(newGraph)

    println("Pregel sample -- Normalized")
    printGraph(newGraph)

    def setMsg2(vertexId: VertexId, value: Object, message: String):Object = {
      if (message != "") {
        println(vertexId.toString+" ===  "+message)
        //var mapa = new scala.collection.mutable.HashMap[Long, Double]
        value match {
          case value: UserPreferences => {
            var user = value.asInstanceOf[UserPreferences]
            for (i <- message.split("&")) {
              val data = i.split(":")
              var friendMap = new HashMap[Long, Double]()
              val numbers = data(1).split(",")
              for (j: Int <- 0 until (numbers.length / 2)) {
                val index = numbers(j * 2)
                val freq = numbers(j * 2 + 1)
                friendMap(index.toLong) = freq.toDouble
              }
              user.friendsPreferences(data(0).toLong) = friendMap

            }
            user.asInstanceOf[Object]
          }
          case _ => value
        }
      } else {//Initial Message used for Transformation
        value match {
          case value: HashMap[Long, Double] => {
            val mapa = value.asInstanceOf[scala.collection.mutable.HashMap[Long, Double]]
            var user = UserPreferences(mapa, new HashMap[Long, HashMap[Long, Double]]())
            println("Attributo Transformado")
            user
          }
          case _ => value
        }
      }
    }

    def sendMsg2(triplet: EdgeTriplet[Object,String] ): Iterator[(VertexId,String)] = {
      //println(triplet.srcId+" --"+triplet.dstId)
      val tuple=(triplet.srcAttr,triplet.dstAttr)

      tuple match {

        case (srcAttr: UserPreferences, dstAttr:UserPreferences)=> {
          println("Tamanio "+dstAttr.friendsPreferences.size)
          if(srcAttr.friendsPreferences.size>0) {

          }
          var str=triplet.srcId+":"
            srcAttr.preferences foreach {case (key, value) => str+=key+","+value+","}
            if (str.endsWith(","))
              str=str.slice(0,str.length()-1)
          println("Ida "+triplet.srcId+" --"+triplet.dstId)
          println(triplet.dstId+"====>  "+str)
          Iterator((triplet.dstId,str))
          //Iterator((triplet.srcId,str))
        }


        case _=>Iterator.empty
      }
    }

    def mergeMsg2(msg1: (String), msg2: (String)): String={
      msg1+"&"+msg2
    }



    def sendMsg22(triplet: EdgeTriplet[Object,String] ): Iterator[(VertexId,String)] = {

      val tuple=(triplet.srcAttr,triplet.dstAttr)

      tuple match {

        case (srcAttr: UserPreferences, dstAttr:UserPreferences)=> {
          var str = triplet.dstId + ":"
          dstAttr.preferences foreach { case (key, value) => str += key + "," + value + "," }
          if (str.endsWith(","))
            str = str.slice(0, str.length() - 1)

          println("Regreso"+triplet.srcId+" --"+triplet.dstId)
          println(triplet.srcId+"====>  "+str)
          Iterator((triplet.srcId, str))
        }
        case _ => Iterator.empty
      }
    }
    val userWithFriendPreferencesIntermediate: Graph[Object, String]= newGraph.pregel("",maxIterations = 1,EdgeDirection.Out)(
      setMsg2, // Vertex Program
      sendMsg2,// vertex received msg
      mergeMsg2// Merge Message
    )
    printGraph(userWithFriendPreferencesIntermediate)
    println("============")
    println("Friend WithPreferences")
    val userWithFriendPreferences: Graph[Object, String]= userWithFriendPreferencesIntermediate.pregel("",maxIterations = 1,EdgeDirection.In)(
      setMsg2, // Vertex Program
      sendMsg22,// vertex received msg
      mergeMsg2// Merge Message
    )

    printGraph(userWithFriendPreferences)

    val recomendationsVertices=makeRecomendations(userWithFriendPreferences)
    val recomendationsGraph=Graph(recomendationsVertices,userWithFriendPreferences.edges)
    println("Recomendations")
    printGraph(recomendationsGraph)



    //////// Clusters
    println("----Analysis with clusters-----")
    val verticesUsersClusters = Array[ ( VertexId, Object) ] (
      ( 100L,UserCluster(new HashMap[Long, HashMap[Long, Double]]())) ,
      ( 200L, UserCluster( new HashMap[Long, HashMap[Long, Double]]())) ,
    )
    val nodesUC= sc.parallelize(verticesUsersClusters)

    val linksUsersCluster = Array(
      Edge( 1L, 100L,"" ),
      Edge( 4L, 100L ,""),
      Edge( 5L, 100L ,""),
      Edge( 2L, 200L ,""),
      Edge( 3L, 200L ,""),
    )
    val edgesUC= sc.parallelize(linksUsersCluster)

    //Create Graph
    val graphUC= Graph(userMapsProperty.vertices.union(nodesUC),userMapsProperty.edges.union(edgesUC) )

    graphUC.vertices.foreach(println)
    graphUC.edges.foreach(println)


    def setMsgInClusterOrUser(vertexId: VertexId, value: Object, message: String):Object = {
      println("------------ID:"+vertexId+"   "+stringOf(value) +"   Message:"+message)
      if (message != "") {
        println(vertexId.toString+" ===  "+message)
        value match {
          case value: UserCluster => {

            for (i <- message.split("&")) {
              val data = i.split(":")
              var friendMap = new HashMap[Long, Double]()
              val numbers = data(1).split(",")
              for (j: Int <- 0 until (numbers.length / 2)) {
                val index = numbers(j * 2)
                val freq = numbers(j * 2 + 1)
                friendMap(index.toLong) = freq.toDouble
              }
              value.friendsPreferences(data(0).toLong) = friendMap

            }
            value.asInstanceOf[Object]
          }
        }
      } else { //Initial Message used for Transformation
          print("Se fue por aqui")
          value match {
            case value: HashMap[Long, Double] => {
              val mapa = value.asInstanceOf[scala.collection.mutable.HashMap[Long, Double]]
              var user = UserPreferences(mapa, new HashMap[Long, HashMap[Long, Double]]())
              println("Attributo Transformado")
              user
            }
            case _ => value
          }
        }
    }
    def sendMsgToCluster(triplet: EdgeTriplet[Object,String] ): Iterator[(VertexId,String)] = {
      //println(triplet.srcId+" --"+triplet.dstId)
      val tuple=(triplet.srcAttr,triplet.dstAttr)

      tuple match {
        case (srcAttr: UserPreferences, dstAttr:UserCluster)=> {
          var str=triplet.srcId+":"
          srcAttr.preferences foreach {case (key, value) => str+=key+","+value+","}
          if (str.endsWith(","))
            str=str.slice(0,str.length()-1)
          println("Ida "+triplet.srcId+" --"+triplet.dstId)
          println(triplet.dstId+"====>  "+str)
          Iterator((triplet.dstId,str))
          //Iterator((triplet.srcId,str))
        }
        case _=>Iterator.empty
      }
    }

    val cluster: Graph[Object, String]= graphUC.pregel("",maxIterations = 1,EdgeDirection.Out)(
      setMsgInClusterOrUser, // Vertex Program
      sendMsgToCluster,// vertex received msg
      mergeMsg2// Merge Message
    )

    printGraph(cluster)
    def setMsgInUser(vertexId: VertexId, value: Object, message: String):Object = {
      println("------------ID:"+vertexId+"   "+stringOf(value) +"   Message:"+message)
      if (message != "") {
        println(vertexId.toString+" ===  "+message)
        value match {
          case value: UserPreferences=> {

            for (i <- message.split("&")) {
              val data = i.split(":")
              println(stringOf(data))
              var friendMap = new HashMap[Long, Double]()
              val numbers = data(1).split(",")
              for (j: Int <- 0 until (numbers.length / 2)) {
                val index = numbers(j * 2)
                val freq = numbers(j * 2 + 1)
                friendMap(index.toLong) = freq.toDouble
              }
              if(vertexId.toLong!=data(0).toLong)
                value.friendsPreferences(data(0).toLong) = friendMap

            }
            value.asInstanceOf[Object]
          }
        }
      }
      value match {
        case value: UserCluster=> {
          (vertexId,value)
        }
        case _ => value
      }

    }



    def alternate(triplet: EdgeTriplet[Object,String] ): Iterator[(VertexId,String)] = {
      println(triplet.srcId+" -- "+triplet.dstId)
      val tuple=(triplet.srcAttr,triplet.dstAttr)

      println(triplet.dstId+": "+stringOf(triplet.dstAttr))
      tuple match {
        case (srcAttr:UserPreferences, dstAttr:(VertexId,UserCluster ))=> {

          var str=""//triplet.srcId+":"
          println( dstAttr)
          dstAttr._2.friendsPreferences foreach {case (friendID, friendMap) =>{
            str+=friendID+":"
            println("Friend: "+friendID)
            friendMap foreach{case (locationId, preference) =>{
              str+=locationId+","+preference+","
            }

            }
            if (str.endsWith(","))
              str=str.slice(0,str.length()-1)
            str+="&"
          }}
          if (str.endsWith("&"))
            str=str.slice(0,str.length()-1)

          println("Ida "+triplet.srcId+" --   "+triplet.dstId)
          println(triplet.dstId+"====>  "+str)
          Iterator((triplet.srcId,str))
        }
        case _=>Iterator.empty
      }
    }

    println("------------------")
    val userWithClusterPref: Graph[Object, String]= cluster.pregel("",maxIterations = 1)(
      setMsgInUser, // Vertex Program
      alternate,// vertex received msg
      mergeMsg2// Merge Message
    )
    println("------------------")


    printGraph(userWithClusterPref)
    val userRecommendationsCluster=makeRecomendations(userWithClusterPref)
    val finalNetworkCluster=Graph(userRecommendationsCluster,userWithClusterPref.edges)
    printGraph(finalNetworkCluster)

//    graphUC.outerJoinVertices(userMapsProperty.vertices){
//            (id, origValue, msgValue ) => origValue match {
//              case  => {
//                var mapa = new scala.collection.mutable.HashMap[Long, Double]
//                for (i <- places.split(",")) {
//                  if (mapa.contains(i.toLong)) mapa(i.toLong) += 1.0
//                  else mapa(i.toLong) = 1.0
//                }
//                mapa
//              }  // vertex received msg
//              case None => origValue
//            }
//          }
    //    graph.collect.foreach {
    //      case ( id, User(  ) ) => println( s"$id" )
    //      case _ =>
    //    }
    spark.stop()
  }
}
