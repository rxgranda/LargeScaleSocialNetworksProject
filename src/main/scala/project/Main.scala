package project


import org.apache.spark.SparkContext
import org.apache.spark.graphx.{VertexId, _}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.HashMap
import org.apache.spark.rdd.RDD

import scala.runtime.ScalaRunTime._
import scala.collection.mutable.ArrayBuffer


//Primitive class to simulate the object to be stored in each Vertex
class VertexProperty()
//Different Classes to store information in each Vertex

//Class used as reference for users in vertex
case class User(val places:Array[Double]) extends VertexProperty
//Class used to store user preferences
case class UserPreferences(var preferences:HashMap[Long, Double],var friendsPreferences:HashMap[Long,HashMap[Long, Double] ]) extends VertexProperty
//Class to Represent each cluster of user as a Node
case class UserCluster(var friendsPreferences:HashMap[Long,HashMap[Long, Double] ]) extends VertexProperty
//Class to store Location Nodes
case class Location(val lat: String, val long: String) extends VertexProperty

object Main {

  /**
    * Method used to print the resulting network in console
    * @param g A graph with Any object in vertices and Strings as Edges
    */
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

  /**
    * This method perform the calculations of the final recomendations given to the users. This takes into account the prefference of each user
    * @param userWithFriendPreferences
    * @return A Graph with recomendations based on information of user's friends
    */
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
            denominator+=value}}
          if(denominator==0.0)
            (id,scores)
          for ( loc <- unvisitedLocations ) {
            var numerator=0.0
            friendHistory foreach {case (frienID, mapPlaces) =>{
              if(mapPlaces.contains(loc))
                numerator+=similarities(frienID)*mapPlaces(loc)
            }}
            var result=numerator/denominator
            if (!result.isNaN && result>0.00000001)
              scores(loc)=result
          }
          println(stringOf((id,scores)))
          (id,scores)
        }
        case _ => (id, attr)
      }
    }
    }
    recomendations.checkpoint()
    recomendations
  }


  /**
    * This method uses Pregel to the analysis of the given graph to provide to user's recommendations based on friends network
    * @param graphWithNormalizedFrequencies
    */
  def recomendationsWithOutClusters(graphWithNormalizedFrequencies:Graph[Object,String]): Unit ={


    def getInformationFromFriendsvertexProgr(vertexId: VertexId, value: Object, message: String):Object = {
      if (message != "") {
        value match {
          case value: UserPreferences => {
            var user = value.asInstanceOf[UserPreferences]
            for (i <- message.split("&")) {
              val data = i.split(":")
              var friendMap = new HashMap[Long, Double]()
              var numbers = Array[String]()
              try {
                numbers=data(1).split(",")
              } catch {
                case e: Exception => {
                  println("id:"+vertexId+"  message:"+message)
                  println(e)
                }
              }
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
            user
          }
          case _ => value
        }
      }
    }

    def sendInformationToFriendsSendMessage(triplet: EdgeTriplet[Object,String] ): Iterator[(VertexId,String)] = {
      val tuple=(triplet.srcAttr,triplet.dstAttr)
      tuple match {
        case (srcAttr: UserPreferences, dstAttr:UserPreferences)=> {
          var str=triplet.srcId+":"
          srcAttr.preferences foreach {case (key, value) => str+=key+","+value+","}
          if (str.endsWith(","))
            str=str.slice(0,str.length()-1)
          Iterator((triplet.dstId,str))
        }
        case _=>Iterator.empty
      }
    }

    def mergeMessage(msg1: (String), msg2: (String)): String={
      msg1+"&"+msg2
    }


    def shareInformationWithFriendsSendMessage(triplet: EdgeTriplet[Object,String] ): Iterator[(VertexId,String)] = {

      val tuple=(triplet.srcAttr,triplet.dstAttr)
      tuple match {

        case (srcAttr: UserPreferences, dstAttr:UserPreferences)=> {
          var str = triplet.dstId + ":"
          dstAttr.preferences foreach { case (key, value) => str += key + "," + value + "," }
          if (str.endsWith(","))
            str = str.slice(0, str.length() - 1)
          Iterator((triplet.srcId, str))
        }
        case _ => Iterator.empty
      }
    }

    val userWithFriendPreferencesIntermediate: Graph[Object, String]= graphWithNormalizedFrequencies.pregel("",maxIterations = 1,EdgeDirection.Out)(
      getInformationFromFriendsvertexProgr, // Vertex Program
      sendInformationToFriendsSendMessage,// vertex received msg
      mergeMessage// Merge Message
    )
    //printGraph(userWithFriendPreferencesIntermediate)
    userWithFriendPreferencesIntermediate.checkpoint()
    println("============")
    println("Friend WithPreferences")
    val userWithFriendPreferences: Graph[Object, String]= userWithFriendPreferencesIntermediate.pregel("",maxIterations = 1,EdgeDirection.In)(
      getInformationFromFriendsvertexProgr, // Vertex Program
      shareInformationWithFriendsSendMessage,// vertex received msg
      mergeMessage// Merge Message
    )
    userWithFriendPreferences.checkpoint()

    //    userWithFriendPreferences.vertices.saveAsTextFile("./resultadoAmigos")
    val justUsers=userWithFriendPreferences.subgraph(
      epred=(triplet: EdgeTriplet[Object,String] )=>{
        val tuple=(triplet.srcAttr,triplet.dstAttr)
        tuple match {
          case (source:UserPreferences,dest:UserPreferences)=>true//if(triplet.srcId<numNodes)true else false
          case _ =>false
        }
      },
      vpred = (vid:VertexId, attr:Object) => {
        attr match {
          case attr: UserPreferences => true//if(vid<numNodes)true else false
          case _=>false
        }
      })

    val recomendationsVertices=makeRecomendations(justUsers)
    val recomendationsGraph=Graph(recomendationsVertices,justUsers.edges)
    recomendationsGraph.vertices.saveAsTextFile("./resultadoTodosAmigosUnaSemana")
    //println("Recomendations")
    printGraph(recomendationsGraph)


  }


  /**
    * This method perform the analysis of the network using cluster information. This method used Pregel to share information within users in the same cluster.
    * @param graphWithNormalizedFrequencies
    */
  def recommendationsWithClusterInformation(graphWithNormalizedFrequencies:Graph[Object,String], sc: SparkContext ): Unit ={
    println("----Analysis with clusters-----")

    //Filter users, remove Location Information
    val justUsers=graphWithNormalizedFrequencies.subgraph(
      epred=(triplet: EdgeTriplet[Object,String] )=>{
        val tuple=(triplet.srcAttr,triplet.dstAttr)
        tuple match {
          case (source:HashMap[Long,Double],dest:HashMap[Long,Double])=>true//if(triplet.srcId<numNodes)true else false
          case _ =>false
        }
      },
      vpred = (vid:VertexId, attr:Object) => {
        attr match {
          case attr: scala.collection.mutable.HashMap[Long,Double] => true//if(vid<numNodes)true else false
          case _=>false
        }
      })

    justUsers.checkpoint()
    //Transform each HashMap into a UserPrefference Object
    val justUsersVertices=justUsers.vertices.mapValues((vertexid,mapa)=>UserPreferences(mapa.asInstanceOf[HashMap[Long,Double]],new HashMap[Long, HashMap[Long, Double]]()).asInstanceOf[Object])
    val ids=justUsersVertices.collect.map(a=>a._1)

    //    val verticesUsersClusters = Array[ ( VertexId, Object) ] (
    //      ( 100L,UserCluster(new HashMap[Long, HashMap[Long, Double]]())) ,
    //      ( 200L, UserCluster( new HashMap[Long, HashMap[Long, Double]]())) ,
    //    )

    val csvC = sc.textFile("../undiaClusters")
    //Create User-Friendship Edges
    val linksUsersCluster = csvC.map { line =>
      val fields = line.split(",")
      Edge(fields(0).toLong, fields(1).toLong*(-1L), "")
    }.filter(e=>ids.contains(e.srcId))
    //Create User nodes
    val verticesUsersClusters: RDD[(Long, Object)] = linksUsersCluster.map(friendShip => (friendShip.dstId, UserCluster(new HashMap[Long, HashMap[Long, Double]]()).asInstanceOf[Object])).distinct

    //val nodesUC= sc.parallelize(verticesUsersClusters)

    //    val linksUsersCluster = Array(
    //      Edge( 1L, 100L,"" ),
    //      Edge( 4L, 100L ,""),
    //      Edge( 5L, 100L ,""),
    //      Edge( 2L, 200L ,""),
    //      Edge( 3L, 200L ,""),
    //    )
    // val edgesUC= sc.parallelize(linksUsersCluster)
    val a=sc.union(justUsers.edges,linksUsersCluster)
    val b=sc.union(justUsersVertices, verticesUsersClusters)
    //Create Graph with users and cluster information
    val graphUC= Graph(b,a )

    graphUC.vertices.take(100).foreach(println)
    graphUC.edges.take(100).foreach(println)

    graphUC.checkpoint()


    def setMsgInClusterOrUser(vertexId: VertexId, value: Object, message: String):Object = {
      //println("------------ID:"+vertexId+"   "+stringOf(value) +"   Message:"+message)
      if (message != "") {
        value match {
          case value: UserCluster => {
            for (i <- message.split("&")) {
              val data = i.split(":")
              var friendMap = new HashMap[Long, Double]()
              //val numbers = data(1).split(",")
              var numbers = Array[String]()
              try {
                numbers=data(1).split(",")
              } catch {
                case e: Exception => {
                  println("id:"+vertexId+"  message:"+message)
                  println(e)
                }
              }
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
        value match {
          case value: HashMap[Long, Double] => {
            val mapa = value.asInstanceOf[scala.collection.mutable.HashMap[Long, Double]]
            var user = UserPreferences(mapa, new HashMap[Long, HashMap[Long, Double]]())
            user
          }
          case _ => value
        }
      }
    }
    def sendMsgToCluster(triplet: EdgeTriplet[Object,String] ): Iterator[(VertexId,String)] = {
      val tuple=(triplet.srcAttr,triplet.dstAttr)
      tuple match {
        case (srcAttr: UserPreferences, dstAttr:UserCluster)=> {
          var str=triplet.srcId+":"
          srcAttr.preferences foreach {case (key, value) => str+=key+","+value+","}
          if (str.endsWith(","))
            str=str.slice(0,str.length()-1)

          if (srcAttr.preferences.size<1){
            Iterator.empty
          }else
            Iterator((triplet.dstId,str))
        }
        case _=>Iterator.empty
      }
    }

    def mergeMessage(msg1: (String), msg2: (String)): String={
      msg1+"&"+msg2
    }

    val cluster: Graph[Object, String]= graphUC.pregel("",maxIterations = 1,EdgeDirection.Out)(
      setMsgInClusterOrUser, // Vertex Program
      sendMsgToCluster,// vertex received msg
      mergeMessage// Merge Message
    )
    cluster.checkpoint()

    printGraph(cluster)
    //Create a new graph with the information collected from users in cluster nodes
    val newGrpah=Graph(cluster.vertices,cluster.edges)

    /// Use message passing to send back information of users that share the same cluster
    val messages2 = newGrpah.aggregateMessages[Object](
      ctx =>{
        ctx.dstAttr match {
          case dstAttr : UserCluster=> {
            var str=""//triplet.srcId+":"
            println(ctx.srcId)///+ stringOf(dstAttr))
            ctx.sendToSrc(dstAttr.friendsPreferences)
          }
          case _=>"a"
        }
      },
      (a,b)=>a+","+b
    )
    messages2.checkpoint()

    ///Perform Reccomendations in each user
    val userWithClusterPref:Graph[Object,String] = cluster.outerJoinVertices(messages2) {
      (vertexId, origValue, friendsPreferences ) => (origValue,friendsPreferences) match {
        case (origValue:UserPreferences,Some(friendsPreferences:HashMap[Long,HashMap[Long,Double]])) => {
          val friendHistory=friendsPreferences
          val selfHistory=origValue.preferences
          var unvisitedLocations = ArrayBuffer[Long]()
          var magnitudeU=0.0
          selfHistory foreach {case (locationID, value) =>magnitudeU+=value*value}
          magnitudeU=math.sqrt(magnitudeU)
          //println(id+":  Umag:"+magnitudeU)
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
                      //println(numerator)
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
            denominator+=value}}
          if(denominator==0.0)
            scores
          for ( loc <- unvisitedLocations ) {
            var numerator=0.0
            friendHistory foreach {case (frienID, mapPlaces) =>{
              if(mapPlaces.contains(loc))
                numerator+=similarities(frienID)*mapPlaces(loc)
            }}
            var result=numerator/denominator
            if (!result.isNaN && result>0.00001)
              scores(loc)=result
          }

          println(stringOf((vertexId,scores)))
          scores
        }  // vertex received msg
        case _ => origValue
      }
    }

    println("------------------")

    userWithClusterPref.checkpoint()
    userWithClusterPref.vertices.saveAsTextFile("./results")
    printGraph(userWithClusterPref)
    val userRecommendationsCluster=makeRecomendations(userWithClusterPref)
    userRecommendationsCluster.checkpoint()
    val finalNetworkCluster=Graph(userRecommendationsCluster,userWithClusterPref.edges)
    finalNetworkCluster.checkpoint()
    printGraph(finalNetworkCluster)

  }

  def main(args: Array[String]): Unit = {
    // Creates a SparkSession.
    val spark = SparkSession
      .builder
      .config("spark.executor.memory", "2g")
      .config("spark.driver.memory", "7g")
      .appName("Project")
      .config("spark.master", "local")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    spark.sparkContext.setCheckpointDir("./chk/")
    val sc = spark.sparkContext
    //Load First CSV
    val csv = sc.textFile("../Edges")
    //Create User-Friendship Edges
    val friendshipEdges = csv.map { line =>
      val fields = line.split("\t")
      Edge(fields(0).toLong, fields(1).toLong, "")
    }
    //Create User nodes
    val users: RDD[(Long, VertexProperty)] = friendshipEdges.map(friendShip => (friendShip.srcId, User(Array()).asInstanceOf[VertexProperty])).distinct
    //Load Second CSV
    val csv2 = sc.textFile("../unaSemanaCheckings")
    //Create User-Location Edges
    val usrLocationEdges = csv2.map { line =>
      val fields = line.split("\t")
      Edge(fields(4).toLong * 1000L,fields(0).toLong, fields(1))
    }

    val locationNodes: RDD[(Long, VertexProperty)] = csv2.map { line =>
      val fields = line.split("\t")
      (fields(4).toLong * 1000L, Location(fields(2), fields(3)).asInstanceOf[VertexProperty])
    }.distinct
    //Just show 5 samples
    println("Just show 5 examples")
    friendshipEdges.take(5).foreach(println)
    users.take(5).foreach(println)
    usrLocationEdges.take(5).foreach(println)
    locationNodes.take(5).foreach(println)
    //Merge nodes and edges of different types
    val nodes = sc.union(users,locationNodes);
    val edges = sc.union(friendshipEdges,usrLocationEdges);

    //    val vertices = Array[ ( VertexId, VertexProperty) ] (
    //      ( 10L, Location("51.903491456","8.636259513")) ,
    //      ( 11L, Location("51.903491456","8.636259513")) ,
    //      ( 12L, Location("51.903491456","8.636259513")) ,
    //      ( 13L, Location("51.903491456","8.636259513")) ,
    //      ( 14L, Location("51.903491456","8.636259513")) ,
    //      ( 1L, User(Array())) ,
    //      ( 2L, User(Array())) ,
    //      ( 3L, User(Array())) ,
    //      ( 4L, User(Array())) ,
    //      ( 5L, User(Array())) ,
    //    )
    //    val nodes= sc.parallelize(vertices)
    //
    //    val links = Array(
    //      Edge( 1L, 4L,"" ),
    //      Edge( 2L, 4L ,""),
    //      Edge( 2L, 5L,"" ),
    //      Edge( 3L, 5L,"" ),
    //      Edge( 10L, 1L,"2010-10-19T23:55:27Z" ),
    //      Edge( 11L, 1L,"2010-10-19T23:55:27Z" ),
    //      Edge( 12L, 2L,"2010-10-19T23:55:27Z" ),
    //      Edge( 13L, 2L,"2010-10-19T23:55:27Z" ),
    //      Edge( 14L, 3L,"2010-10-19T23:55:27Z" ),
    //      Edge( 11L, 4L,"2010-10-19T23:55:27Z" ),
    //      Edge( 12L, 4L,"2010-10-19T23:55:27Z" ),
    //      Edge( 12L, 5L,"2010-10-19T23:55:27Z" ),
    //      Edge( 14L, 5L,"2010-10-19T23:55:27Z" ),
    //      Edge( 10L, 1L,"2010-10-19T23:55:27Z" ),
    //      Edge( 13L, 3L,"2010-10-19T23:55:27Z" ),
    //    )
    //    val edges= sc.parallelize(links)
    //Sample 5
    nodes.take(5).foreach(println)
    edges.take(5).foreach(println)
    //Create the Initial Graph
    val graph= Graph(nodes, edges)

   //Transform each node into a HashMapCollection to collect information of places for each user
    val newVertices = graph.vertices.map { case (id, attr) => {
      attr match {
        case attr: User => (id, new scala.collection.mutable.HashMap[Long, Double])
        case _ => (id, attr)
      }
    }
    }
    /// Create a Graph with each HashMap
    val userMapsProperty=Graph(newVertices,graph.edges)


    //// Use Pregel to pass the information of Each Location to the Users that have visited a given Place
    def receiveMessageFromLocationVertexProgram(vertexId: VertexId, value: Object, message: String) = {
      message match {
        case message:String => {
          //print(message)
          if (message!="") {
            var mapa =value.asInstanceOf[scala.collection.mutable.HashMap[Long, Double]]
            try {
              for (i <- message.split(",")) {
                if (mapa.contains(i.toLong)) mapa(i.toLong) += 1.0
                else mapa(i.toLong) = 1.0
              }
            } catch {
              case e: Exception => {
                println("id:"+vertexId+"  message:"+message)
                println(e)
              }
            }
            mapa
          }else{
            value
          }
        }
        case _ => value
      }
    }
    def sendMessageFromLocationToUsers(triplet: EdgeTriplet[Object,String] ): Iterator[(VertexId,String)] = {
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
      receiveMessageFromLocationVertexProgram, // Vertex Program
      sendMessageFromLocationToUsers,// vertex received msg
      mergeMsg// Merge Message
    )

    /// Normalize the frequency of users visits to each Location In each User vertex
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
    val graphWithNormalizedFrequencies=Graph(userMapsPropertyNormalized,userMapsPropertyAccum.edges)
    println("Pregel Normalized Frequencies")
    graphWithNormalizedFrequencies.vertices.take(100).foreach(println)
    //printGraph(newGraph)

    /// Call first Algorithm  based on Information of friends without clusters
    recomendationsWithOutClusters(graphWithNormalizedFrequencies)

    /// Call second Algorithm  based on Information of friends with clusters
    recommendationsWithClusterInformation(graphWithNormalizedFrequencies,sc)

    print("End")

    spark.stop()
  }
}
