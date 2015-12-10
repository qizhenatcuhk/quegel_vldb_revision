//
/**
 * @file
 * @author  Qizhen Zhang <qizhen_cuhk@outlook.com>
 * @version 1.0
 *
 * @section LICENSE
 *
 * Copyright [2012] [Qizhen Zhang, Da Yan, James Cheng / The Chinese University of Hong Kong]
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package quegel
import com.esotericsoftware.kryo.Kryo
import org.apache.spark.SparkContext._
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag
import scala.Array.canBuildFrom
import org.apache.log4j.Logger
import org.apache.log4j.Level


object bfs {

  val SEPARATOR = "[\t ]"

  def loadUndirectedGraph[VD: ClassTag, ED: ClassTag](sc: SparkContext, path: String, defaultEdgeAttr: ED, defaultVetexAttr: VD): Graph[VD, ED] =
  {
    val textRDD = sc.textFile(path);
    val edge = textRDD.flatMap(
      line => {
        val numbers = line.split(SEPARATOR);
        val srcId: VertexId = numbers(0).trim.toLong;
        numbers.slice(2, numbers.size).map(num => Edge(srcId, num.trim.toLong, defaultEdgeAttr)).filter(p => p.srcId != p.dstId)
      })
    Graph.fromEdges[VD, ED](edge, defaultVetexAttr);
  }

  def loadUndirectedGraph_Adj[VD: ClassTag, ED: ClassTag](sc: SparkContext, path: String, defaultEdgeAttr: ED, defaultVetexAttr: VD): Graph[VD, ED]=
  {
    val textRDD=sc.textFile(path)
    println("Loading: "+path)
    println("Lines: "+textRDD.count())
    val edge=textRDD.flatMap(
      line=>{
        val numbers=line.split(" ")
        val srcId: VertexId = numbers(0).split(SEPARATOR)(0).trim.toLong;
        numbers.slice(1, numbers.size).map(num=>Edge(srcId, num.trim.toLong, defaultEdgeAttr)).filter(p=>p.srcId!=p.dstId)
      }
    )
    println("Edge size:"+edge.count)
    Graph.fromEdges[VD, ED](edge, defaultVetexAttr);
  }

  /**
   *The difference from undirected graph loading is that the in-edgies should be ignored
   * */
  def loadDirectedGraph[VD: ClassTag, ED: ClassTag](sc: SparkContext, path: String, defaultEdgeAttr: ED, defaultVetexAttr: VD): Graph[VD, ED] =
  {
    val textRDD = sc.textFile(path);
    val edge = textRDD.flatMap(
      line => {
        val numbers = line.split(SEPARATOR);
        val srcId: VertexId = numbers(0).trim.toLong;
        val inNeighborsNum = numbers(1).trim.toInt;
        numbers.slice(3 + inNeighborsNum, numbers.size).map(num => Edge(srcId, num.trim.toLong, defaultEdgeAttr)).filter(p => p.srcId != p.dstId)
      })
    Graph.fromEdges[VD, ED](edge, defaultVetexAttr);
  }

  //load hubs from file
  def loadHubs(sc: SparkContext, path: String): RDD[Long]=
  {
    val textRDD = sc.textFile(path)
    textRDD.flatMap(line=>List(line.split(" ")(0).trim.toLong))
  }

  //load queries from file
  def loadQueries(sc: SparkContext, path: String): RDD[(Int, Int, Int)]=
  {
    val queryRDD=sc.textFile(path)
    queryRDD.flatMap(line=>List((line.split(" ")(0).trim.toInt, line.split(" ")(1).trim.toInt, line.split(" ")(2).trim.toInt)))
  }

  def IndexingHub2(sc: SparkContext, inputPath: String, topVerticesPath: String, outputPath: String): (Double, Double, Double)=
  {
    var startTime = System.currentTimeMillis
    println("Loading graph in file:"+inputPath)
    val graph = loadUndirectedGraph_Adj(sc, inputPath, (Int.MaxValue, false), (Int.MaxValue, false)).partitionBy(PartitionStrategy.EdgePartition1D).cache()
    println("Graph loaded! Graph size:"+graph.vertices.count)
    println("Loading hubs in file:"+topVerticesPath)
    val hubs=loadHubs(sc, topVerticesPath)
    var hubs_copy=hubs.collect;
    println("Hubs loaded! "+hubs.count()+" hubs.")
    var loadtime = System.currentTimeMillis - startTime

    var computeTime = 0.0
    var dumpTime = 0.0

    for(i<-0 until hubs_copy.length)
    {
      var hub=hubs_copy(i)
      println("Processing hub:"+hub)
      startTime = System.currentTimeMillis

      if(hubs_copy.find(id=>id==0)==None)
        println("null test: "+hub+" ")
      else
        println("heheda")
      println(graph.vertices.count+" vertices!")

      println("Mapping...!")

      var idx_sssp=graph.mapVertices((id,_)=>if(id==hub) (0, false) else if(hubs_copy.find(_id=>_id==id)!=None) (Int.MaxValue, true) else (Int.MaxValue, false))

      println("Map vertices OK!")

      var newVisited:Long =1
      var superstep=0

      while(newVisited>0)
      {
        superstep+=1;

        var defMsg=(Int.MaxValue, false)
        idx_sssp=idx_sssp.pregel(defMsg, 1)(
          (id, vv, newvv)=>if(vv._1>newvv._1||(vv._1==newvv._1&&(!vv._2)&&newvv._2)) newvv else vv,
          triplet=>{//Send Message
            if(triplet.srcAttr._1 != Int.MaxValue && triplet.dstAttr._1 == Int.MaxValue){
              //              println("Superstep="+superstep+" id="+triplet.srcId+" sends a msg to "+triplet.dstId)
              Iterator((triplet.dstId, (superstep, triplet.srcAttr._2)))
            }else{
              Iterator.empty
            }
          },
          (a, b)=> if(a._2) a else b
        )
        newVisited = idx_sssp.vertices.filter{case(id, (distance, preH)) => distance == superstep}.count
        println("updated: new visited:"+newVisited)
      }

      var curTime=System.currentTimeMillis - startTime;

      startTime = System.currentTimeMillis
      val curOutputPath = outputPath +"_hub"+ hub

      val result = idx_sssp.vertices.flatMap{
        case (id, vv)=>if(vv._1==Int.MaxValue) List(id+" -1") else if(!vv._2) List(id+" "+vv._1) else if(hubs_copy.find(_id=>_id==id)!=None) List(id+" "+vv._1) else List(id+" -1")
      }
      result.saveAsTextFile(curOutputPath)
      dumpTime += System.currentTimeMillis - startTime
    }

    (loadtime, computeTime, dumpTime)
  }

  def SingleSourceBFS(sc: SparkContext, inputPath: String, outputPath: String): (Double, Double, Double) = {
    var startTime = System.currentTimeMillis
    val graph = loadUndirectedGraph(sc, inputPath, 1, 1).partitionBy(PartitionStrategy.EdgePartition1D)
    val loadtime = System.currentTimeMillis - startTime

    var computetime = 0.0
    var dumpTime = 0.0

    for (i <- 0 until SOURCE_LIST.length) {
      val SOURCE_VERTEX = SOURCE_LIST(i)
      val DEST_VERTEX = DEST_LIST(i)

      System.out.println("Query #" + i + " : Source[ " + SOURCE_VERTEX +" ] Dest[ " + DEST_VERTEX +" ]." )

      startTime = System.currentTimeMillis

      var bfsGraph = graph.mapVertices((id, _) => if (id == SOURCE_VERTEX) 0 else Int.MaxValue)

      var newVisited: Long = 1
      var found = false
      var superstep = 0

      while (newVisited > 0 && !found) {

        superstep += 1

        bfsGraph = bfsGraph.pregel(Int.MaxValue, 1)(
          (id, dist, newDist) => math.min(dist, newDist), // Vertex Program
          triplet => { // Send Message
            if (triplet.srcAttr != Int.MaxValue && triplet.dstAttr == Int.MaxValue) {
              Iterator((triplet.dstId, superstep))
            } else {
              Iterator.empty
            }
          },
          (a, b) => a // Merge Message
        )

        newVisited = bfsGraph.vertices.filter(f => f._2 == superstep).count
        found = bfsGraph.vertices.filter { case (id, dis) => id == DEST_VERTEX && dis != Int.MaxValue }.count != 0
      }

      computetime += System.currentTimeMillis - startTime

      System.out.println("Hub #" + i + " Time: " + (System.currentTimeMillis - startTime )) 

      startTime = System.currentTimeMillis
      val curOutputPath = outputPath + "_" + i

      val result = bfsGraph.vertices.filter { case (id, dis) => id == DEST_VERTEX }
      result.saveAsTextFile(curOutputPath)
      dumpTime += System.currentTimeMillis - startTime

    }
    (loadtime, computetime, dumpTime)
  }

  def BiBFS(sc: SparkContext, inputPath: String, outputPath: String, queryPath: String): (Double, Double, Double) = {
    var startTime = System.currentTimeMillis
    val graph = loadDirectedGraph(sc, inputPath, (1, 1), 1).partitionBy(PartitionStrategy.RandomVertexCut)
    val loadtime = System.currentTimeMillis - startTime
    val queries=loadQueries(sc, queryPath).collect

    var computetime = 0.0
    var dumpTime = 0.0

    for (i <- 0 until queries.length) {
      val SOURCE_VERTEX = queries(i)._1
      val DEST_VERTEX = queries(i)._2

      println("Processing query"+i+":<"+SOURCE_VERTEX+","+DEST_VERTEX+">")

      startTime = System.currentTimeMillis

      var bfsGraph = graph.mapVertices((id, _) =>
          id match {
            case SOURCE_VERTEX => (0, Int.MaxValue)
            case DEST_VERTEX => (Int.MaxValue, 0)
            case _ => (Int.MaxValue, Int.MaxValue)
          })
      println("Building graph cosumes: "+(System.currentTimeMillis-startTime)+"ms")
      startTime = System.currentTimeMillis

      var forwardVisited: Long = 1
      var backwardVisited: Long = 1

      var found = false
      var superstep = 0

      while (forwardVisited > 0 && forwardVisited > 0 && !found) {
        superstep += 1


        //forward
        bfsGraph = bfsGraph.pregel(Int.MaxValue, 1)(
          (id, dist, newDist) => (math.min(dist._1, newDist), dist._2), // Vertex Program
          triplet => { // Send Message
            if (triplet.srcAttr._1 != Int.MaxValue && triplet.dstAttr._1 == Int.MaxValue) {
              Iterator((triplet.dstId, superstep))
            } else {
              Iterator.empty
            }
          },
          (a, b) => a // Merge Message
        )
        //backward
        bfsGraph = bfsGraph.pregel(Int.MaxValue, 1)(
          (id, dist, newDist) => (dist._1, math.min(dist._2, newDist)), // Vertex Program
          triplet => { // Send Message
            if (triplet.dstAttr._2 != Int.MaxValue && triplet.srcAttr._2 == Int.MaxValue) {
              Iterator((triplet.srcId, superstep))
            } else {
              Iterator.empty
            }
          },
          (a, b) => a // Merge Message
        )

        forwardVisited = bfsGraph.vertices.filter(f => f._2._1 == superstep).count
        backwardVisited = bfsGraph.vertices.filter(f => f._2._2 == superstep).count

        var newCount = bfsGraph.vertices.filter{case (_, dis) => dis._1 == superstep || dis._2 == superstep }.count
        found=bfsGraph.vertices.filter { case (_, dis) => dis._1 != Int.MaxValue && dis._2 != Int.MaxValue }.count!=0
        println("iteration "+superstep+".... with "+newCount+" new vertices")
      }

      computetime += System.currentTimeMillis - startTime
      val cur_computetime=System.currentTimeMillis-startTime

      println("The time consumption of this query is:"+cur_computetime);

      startTime = System.currentTimeMillis
      val curOutputPath = outputPath + "_" + i

      var result = -1

      if (found) {
        result = bfsGraph.vertices.flatMap(attr => {
          val fdist = attr._2._1
          val bdist = attr._2._2

          if (fdist == Int.MaxValue || bdist == Int.MaxValue)
            Iterator.empty
          else
            Iterator(fdist + bdist)
        }).reduce((a, b) => math.min(a, b))

      }

      sc.parallelize(Seq((DEST_VERTEX, result)), 1).saveAsTextFile(curOutputPath)

      dumpTime += System.currentTimeMillis - startTime

    }
    (loadtime, computetime, dumpTime)
  }

  def Hub2(sc: SparkContext, inputPath: String, outputPath: String, queryFile: String, hubFile: String): (Double, Double, Double) = {
    var startTime = System.currentTimeMillis
    val loadtime = System.currentTimeMillis - startTime
    val queries=loadQueries(sc, queryFile).collect
    val hubs=loadHubs(sc, hubFile).collect.toSet
    val graph = loadDirectedGraph(sc, inputPath, (1, 1, false), 1).partitionBy(PartitionStrategy.RandomVertexCut)

    var computetime = 0.0
    var dumpTime = 0.0

    for (i <- 0 until queries.length) {
      val SOURCE_VERTEX = queries(i)._1
      val DEST_VERTEX = queries(i)._2
      val UPPER_BOUND=queries(i)._3

      println("Begin to process query"+i+":<"+SOURCE_VERTEX+","+DEST_VERTEX+"> with upper bound = "+UPPER_BOUND)

      var superStepUpperBound=(UPPER_BOUND+1)/2//upper bound of super steps
      if(UPPER_BOUND==2147483647) superStepUpperBound=100;

      startTime = System.currentTimeMillis

      var bfsGraph = graph.mapVertices((id, _) =>
          id match {
            case SOURCE_VERTEX => if(hubs.contains(id)) (0, Int.MaxValue, true) else (0, Int.MaxValue, false)
            case DEST_VERTEX => if(hubs.contains(id)) (Int.MaxValue, 0, true) else (Int.MaxValue, 0, false)
            case _ => if(hubs.contains(id)) (Int.MaxValue, Int.MaxValue, true) else (Int.MaxValue, Int.MaxValue, false)
          })
      println("Building graph cosumes: "+(System.currentTimeMillis-startTime)+"ms")

      var forwardVisited: Long = 1
      var backwardVisited: Long = 1

      var found = false
      var superstep = 0

      while (forwardVisited > 0 && forwardVisited > 0 && !found && superstep<superStepUpperBound) {
        superstep += 1

        //forward
        bfsGraph = bfsGraph.pregel(Int.MaxValue, 1)(
          (id, dist, newDist) => (math.min(dist._1, newDist), dist._2, false), // Vertex Program
          triplet => { // Send Message
            if (!triplet.dstAttr._3 && triplet.srcAttr._1 != Int.MaxValue && triplet.dstAttr._1 == Int.MaxValue) {
              Iterator((triplet.dstId, superstep))
            } else {
              Iterator.empty
            }
            },
            (a, b) => a // Merge Message
          )

        //backward
        bfsGraph = bfsGraph.pregel(Int.MaxValue, 1)(
          (id, dist, newDist) => (dist._1, math.min(dist._2, newDist), false), // Vertex Program
          triplet => { // Send Message
            if (!triplet.srcAttr._3 && triplet.dstAttr._2 != Int.MaxValue && triplet.srcAttr._2 == Int.MaxValue) {
              Iterator((triplet.srcId, superstep))
            } else {
              Iterator.empty
            }
            },
            (a, b) => a // Merge Message
          )

        forwardVisited = bfsGraph.vertices.filter(f => f._2._1 == superstep).count
        backwardVisited = bfsGraph.vertices.filter(f => f._2._2 == superstep).count

        var newCount = bfsGraph.vertices.filter{case (_, dis)=>dis._1==superstep||dis._2==superstep}.count
        found=bfsGraph.vertices.filter { case (_, dis) => dis._1 != Int.MaxValue && dis._2 != Int.MaxValue }.count!=0
        println("iteration "+superstep+".... with "+newCount+" new vertices")
            }

            computetime += System.currentTimeMillis - startTime

            startTime = System.currentTimeMillis
            val curOutputPath = outputPath + "_" + i

            var result = -1

            if (found) {
              result = bfsGraph.vertices.flatMap(attr => {
                val fdist = attr._2._1
                val bdist = attr._2._2

                if (fdist == Int.MaxValue || bdist == Int.MaxValue)
                  Iterator.empty
                else
                  Iterator(fdist + bdist)
              }).reduce((a, b) => math.min(a, b))

            }
            else result=UPPER_BOUND

            System.out.println("Query"+ i+":<"+SOURCE_VERTEX+"," + DEST_VERTEX + "> = " + result+", consuming "+(computetime/1000.0)+"s.")
            //sc.parallelize(Seq((DEST_VERTEX, result)), 1).saveAsTextFile(curOutputPath)

            dumpTime += System.currentTimeMillis - startTime
          }
          (loadtime, computetime, dumpTime)
	}

            def Pregel_Qizhen[VD: ClassTag, ED: ClassTag, A: ClassTag]
            (graph: Graph[VD, ED],
              initialMsg: A,
              maxIterations: Int = Int.MaxValue,
              activeDirection: EdgeDirection = EdgeDirection.Either)
            (vprog: (VertexId, VD, A) => VD,
              sendMsg: EdgeTriplet[VD, ED] => Iterator[(VertexId, A)],
              mergeMsg: (A, A) => A)
            : Graph[VD, ED] =
            {
              var g = graph.mapVertices((vid, vdata) => vprog(vid, vdata, initialMsg)).cache()
              var messages = g.mapReduceTriplets(sendMsg, mergeMsg)
              var activeMessages = messages.count()
              var prevG: Graph[VD, ED] = null
              var i = 0
              var found=false
              while (activeMessages > 0 && i < maxIterations && !found) {
                println("Iteration "+i+"...")
                val newVerts = g.vertices.innerJoin(messages)(vprog).cache()
                prevG = g
                g = g.outerJoinVertices(newVerts) { (vid, old, newOpt) => newOpt.getOrElse(old) }
                g.cache()

                val oldMessages = messages
                messages = g.mapReduceTriplets(sendMsg, mergeMsg, Some((newVerts, activeDirection))).cache()
                activeMessages = messages.count()
                oldMessages.unpersist(blocking=false)
                newVerts.unpersist(blocking=false)
                prevG.unpersistVertices(blocking=false)

                found=g.vertices.filter{ case (id, (ld:Int, rd:Int, md:Int, ishub:Boolean)) => md < Int.MaxValue }.count>0

                i += 1
              }
              g
            }


            def Pregel_Qizhen_BiBFS[VD: ClassTag, ED: ClassTag, A: ClassTag]
            (graph: Graph[VD, ED],
              initialMsg: A,
              maxIterations: Int = Int.MaxValue,
              activeDirection: EdgeDirection = EdgeDirection.Either)
            (vprog: (VertexId, VD, A) => VD,
              sendMsg: EdgeTriplet[VD, ED] => Iterator[(VertexId, A)],
              mergeMsg: (A, A) => A)
            : Graph[VD, ED] =
            {
              var g = graph.mapVertices((vid, vdata) => vprog(vid, vdata, initialMsg)).cache()
              var messages = g.mapReduceTriplets(sendMsg, mergeMsg)
              var activeMessages = messages.count()
              var prevG: Graph[VD, ED] = null
              var i = 0
              var found=false
              while (activeMessages > 0 && i < maxIterations && !found) {
                println("Iteration "+i+"...")
                val newVerts = g.vertices.innerJoin(messages)(vprog).cache()
                prevG = g
                g = g.outerJoinVertices(newVerts) { (vid, old, newOpt) => newOpt.getOrElse(old) }
                g.cache()

                val oldMessages = messages
                messages = g.mapReduceTriplets(sendMsg, mergeMsg, Some((newVerts, activeDirection))).cache()
                activeMessages = messages.count()
                oldMessages.unpersist(blocking=false)
                newVerts.unpersist(blocking=false)
                prevG.unpersistVertices(blocking=false)

                found=g.vertices.filter{ case (id, (ld:Int, rd:Int, md:Int)) => md < Int.MaxValue }.count>0

                i += 1
              }
              g
            }

        def Pregel_Qizhen_BFS[VD: ClassTag, ED: ClassTag, A: ClassTag]
            (graph: Graph[VD, ED],
              initialMsg: A,
              maxIterations: Int = Int.MaxValue,
              activeDirection: EdgeDirection = EdgeDirection.Either)
            (vprog: (VertexId, VD, A) => VD,
              sendMsg: EdgeTriplet[VD, ED] => Iterator[(VertexId, A)],
              mergeMsg: (A, A) => A)
            : Graph[VD, ED] =
            {
              var g = graph.mapVertices((vid, vdata) => vprog(vid, vdata, initialMsg)).cache()
              var messages = g.mapReduceTriplets(sendMsg, mergeMsg)
              var activeMessages = messages.count()
              var prevG: Graph[VD, ED] = null
              var i = 0
              var found=false
              while (activeMessages > 0 && i < maxIterations && !found) {
                println("Iteration "+i+"...")
                val newVerts = g.vertices.innerJoin(messages)(vprog).cache()
                prevG = g
                g = g.outerJoinVertices(newVerts) { (vid, old, newOpt) => newOpt.getOrElse(old) }
                g.cache()

                val oldMessages = messages
                messages = g.mapReduceTriplets(sendMsg, mergeMsg, Some((newVerts, activeDirection))).cache()
                activeMessages = messages.count()
                oldMessages.unpersist(blocking=false)
                newVerts.unpersist(blocking=false)
                prevG.unpersistVertices(blocking=false)

                found=g.vertices.filter{ case (id, (dist:Int, isDest:Boolean)) => isDest && dist < Int.MaxValue }.count>0
                if(found) println("Find! dist="+(i+1))

                i += 1
              }

              g
            }

            def bibfs_new_vprog(vertexId: VertexId, value: (Int, Int, Int), message: (Int, Int)): (Int, Int, Int)={

              val lDist=math.min(message._1, value._1)
              val rDist=math.min(message._2, value._2)
              var mDist=value._3

              if(lDist<Int.MaxValue&&rDist<Int.MaxValue)
              {
                mDist=lDist+rDist
              }
              (lDist,rDist,mDist)
            }

            def  bibfs_new_sendMsg(triplet: EdgeTriplet[(Int, Int, Int), Boolean]): Iterator[(VertexId, (Int, Int))]={
              if(triplet.srcAttr._1<Int.MaxValue && triplet.dstAttr._1==Int.MaxValue)
              {
                Iterator((triplet.dstId, (triplet.srcAttr._1+1, Int.MaxValue)))
              }
              else if(triplet.dstAttr._2<Int.MaxValue && triplet.srcAttr._2==Int.MaxValue)
              {
                Iterator((triplet.srcId, (Int.MaxValue, triplet.dstAttr._2+1)))
              }
              else
              {
                Iterator.empty
              }
            }

            def bibfs_new_mergeMsg(msg1: (Int, Int), msg2: (Int, Int)): (Int, Int)=(math.min(msg1._1, msg2._1), math.min(msg1._2, msg2._2))

            def BiBFS_New(sc: SparkContext, inputPath: String, outputPath: String, queryFile: String): (Double, Double, Double) = {
              var startTime = System.currentTimeMillis
              val queries=loadQueries(sc, queryFile).collect
              val graph = loadDirectedGraph(sc, inputPath, true, (1, 1, 1)).partitionBy(PartitionStrategy.RandomVertexCut)
              val loadtime = System.currentTimeMillis - startTime

              var computetime = 0.0
              var dumpTime = 0.0

              for (i <- 0 until queries.length) {
                val SOURCE_VERTEX = queries(i)._1
                val DEST_VERTEX = queries(i)._2

                println("Begin to process query"+i+":<"+SOURCE_VERTEX+","+DEST_VERTEX+">")

                startTime = System.currentTimeMillis

                var bfsGraph = graph.mapVertices((id, _) =>
                    id match {
                      case SOURCE_VERTEX => (0, Int.MaxValue, Int.MaxValue)
                      case DEST_VERTEX => (Int.MaxValue, 0, Int.MaxValue)
                      case _ => (Int.MaxValue, Int.MaxValue, Int.MaxValue)
                    })

                startTime = System.currentTimeMillis

                val resGraph=Pregel_Qizhen_BiBFS(bfsGraph, (Int.MaxValue, Int.MaxValue), 100)(
                  bibfs_new_vprog,
                  bibfs_new_sendMsg,
                  bibfs_new_mergeMsg)

                val metCount=resGraph.vertices.filter(f=>f._2._3<Int.MaxValue).count

                val cur_computetime = System.currentTimeMillis - startTime
                computetime+=cur_computetime

                val curOutputPath = outputPath + "_" + i

                //sc.parallelize(Seq((DEST_VERTEX, result)), 1).saveAsTextFile(curOutputPath)

                dumpTime += System.currentTimeMillis - startTime

              }
              (loadtime, computetime, dumpTime)
            }

            def bfs_new_vprog(vertexId: VertexId, value: (Int, Boolean), message: Int): (Int, Boolean)= (math.min(value._1, message), value._2)

            def bfs_new_sendMsg(triplet: EdgeTriplet[(Int, Boolean), Boolean]): Iterator[(VertexId, Int)]={
              if(triplet.srcAttr._1<Int.MaxValue && triplet.dstAttr._1==Int.MaxValue)
              {
                Iterator((triplet.dstId, triplet.srcAttr._1+1))
              }
              else
              {
                Iterator.empty
              }
            }

            def bfs_new_mergeMsg(msg1: Int, msg2: Int): Int=math.min(msg1, msg2)

            def BFS_New(sc: SparkContext, inputPath: String, outputPath: String, queryFile: String): (Double, Double, Double) = {
              var startTime = System.currentTimeMillis
              val queries=loadQueries(sc, queryFile).collect
              val graph = loadDirectedGraph(sc, inputPath, true, (1, false)).partitionBy(PartitionStrategy.RandomVertexCut)
              val loadtime = System.currentTimeMillis - startTime

              var computetime = 0.0
              var dumpTime = 0.0

              for (i <- 0 until queries.length) {
                val SOURCE_VERTEX = queries(i)._1
                val DEST_VERTEX = queries(i)._2

                println("Begin to process query"+i+":<"+SOURCE_VERTEX+","+DEST_VERTEX+"> by new BFS.")

                startTime = System.currentTimeMillis

                var bfsGraph = graph.mapVertices((id, _) =>
                    id match {
                      case SOURCE_VERTEX => (0, false)
                      case DEST_VERTEX => (Int.MaxValue, true)
                      case _ => (Int.MaxValue, false)
                    })

                println("Building graph cosumes: "+(System.currentTimeMillis-startTime)+"ms--")

                startTime = System.currentTimeMillis

                val resGraph=Pregel_Qizhen_BFS(bfsGraph, Int.MaxValue, 100)(
                  bfs_new_vprog,
                  bfs_new_sendMsg,
                  bfs_new_mergeMsg)

                println("process finished! collecting...")
                val metCount=resGraph.vertices.filter(f=> f._2._2 && f._2._1<Int.MaxValue).count
                println("collect finished! met count = "+metCount)

                val cur_computetime = System.currentTimeMillis - startTime
                computetime+=cur_computetime

                val curOutputPath = outputPath + "_" + i

                dumpTime += System.currentTimeMillis - startTime

              }
              (loadtime, computetime, dumpTime)
            }


            def hub2_new_vprog(vertexId: VertexId, value: (Int, Int, Int, Boolean), message: (Int, Int)): (Int, Int, Int, Boolean)={

              val lDist=math.min(message._1, value._1)
              val rDist=math.min(message._2, value._2)
              var mDist=value._3
              if(lDist<Int.MaxValue&&rDist<Int.MaxValue)
              {
                mDist=lDist+rDist
              }
              (lDist,rDist,mDist, value._4)
            }

            def hub2_new_sendMsg(triplet: EdgeTriplet[(Int, Int, Int, Boolean), Boolean]): Iterator[(VertexId, (Int, Int))]={
                if(triplet.srcAttr._1<Int.MaxValue && !(triplet.dstAttr._4) && triplet.dstAttr._1==Int.MaxValue)
                {
                  Iterator((triplet.dstId, (triplet.srcAttr._1+1, Int.MaxValue)))
                }
                else if(triplet.dstAttr._2<Int.MaxValue && !(triplet.srcAttr._4) && triplet.srcAttr._2==Int.MaxValue)
                {
                  Iterator((triplet.srcId, (Int.MaxValue, triplet.dstAttr._2+1)))
                }
                else Iterator.empty
            }

            def hub2_new_mergeMsg(msg1: (Int, Int), msg2: (Int, Int)): (Int, Int)=(math.min(msg1._1, msg2._1), math.min(msg1._2, msg2._2))

            def Hub2_New(sc: SparkContext, inputPath: String, outputPath: String, queryFile: String, hubFile: String): (Double, Double, Double) = {
              var startTime = System.currentTimeMillis
              val queries=loadQueries(sc, queryFile).collect
              val hubs=loadHubs(sc, hubFile).collect.toSet
              val graph = loadDirectedGraph(sc, inputPath, true, (1, 1, 1, false)).partitionBy(PartitionStrategy.RandomVertexCut)
              val loadtime = System.currentTimeMillis - startTime

              var computetime = 0.0
              var dumpTime = 0.0

              for (i <- 0 until queries.length) {
                val SOURCE_VERTEX = queries(i)._1
                val DEST_VERTEX = queries(i)._2
                val UPPER_BOUND=queries(i)._3

                println("Begin to process query"+i+":<"+SOURCE_VERTEX+","+DEST_VERTEX+"> with upper bound = "+UPPER_BOUND)

                var superStepUpperBound=(UPPER_BOUND+1)/2//upper bound of super steps
                if(UPPER_BOUND==2147483647) superStepUpperBound=100

                startTime = System.currentTimeMillis

                var bfsGraph = graph.mapVertices((id, _) =>
                    id match {
                      case SOURCE_VERTEX =>if(hubs.contains(id)) (0, Int.MaxValue, Int.MaxValue, true) else (0, Int.MaxValue, Int.MaxValue, false)
                      case DEST_VERTEX =>if(hubs.contains(id)) (Int.MaxValue, 0, Int.MaxValue, true) else (Int.MaxValue, 0, Int.MaxValue, false)
                      case _ => if(hubs.contains(id)) (Int.MaxValue, Int.MaxValue, Int.MaxValue, true) else (Int.MaxValue, Int.MaxValue, Int.MaxValue, false)
                    })
                println("Building graph cosumes: "+(System.currentTimeMillis-startTime)+"ms")

                startTime = System.currentTimeMillis

                val resGraph=Pregel_Qizhen(bfsGraph,(Int.MaxValue, Int.MaxValue), superStepUpperBound)(
                  hub2_new_vprog,hub2_new_sendMsg,hub2_new_mergeMsg)

                val met=resGraph.vertices.filter(f=>f._2._3<Int.MaxValue).count
                println("processing finished! met="+met)

                val cur_computetime = System.currentTimeMillis - startTime
                computetime+=cur_computetime

                val curOutputPath = outputPath + "_" + i

                var result = UPPER_BOUND

                System.out.println("Query"+ i+":<"+SOURCE_VERTEX+"," + DEST_VERTEX + "> = " + result+", consuming "+(cur_computetime/1000.0)+"s.")
                //sc.parallelize(Seq((DEST_VERTEX, result)), 1).saveAsTextFile(curOutputPath)

                dumpTime += System.currentTimeMillis - startTime

              }
              (loadtime, computetime, dumpTime)
            }


            def main(args: Array[String]) {
              //    Logger.getLogger("org").setLevel(Level.OFF)
              //    Logger.getLogger("akka").setLevel(Level.OFF)

              System.out.println("Begin to run!")

              val conf=new SparkConf().setAppName("bfs")
              conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
              val sc = new SparkContext(conf)

              val inputPath = args(0)
              val outputPath = args(1)
              val cmd = args(2)

              val times =
                cmd match {
                  case "bfs" => {
                    SingleSourceBFS(sc, inputPath, outputPath)
                  }
                  case "biBfs" => {
                    val queriesFile=args(3)
                    BiBFS(sc, inputPath, outputPath, queriesFile)
                  }
                  case "bibfs_new" =>{
                    val queriesFile=args(3)
                    BiBFS_New(sc, inputPath, outputPath, queriesFile)
                  }
                  case "bfs_new" =>{
                    val queriesFile=args(3)
                    BFS_New(sc, inputPath, outputPath, queriesFile)
                  }
                  case "indexing_hub2"=>{
                    val hubsFile=args(3)
                    IndexingHub2(sc, inputPath, hubsFile, outputPath)
                  }
                  case "hub2"=>{
                    val hubsFile=args(3)
                    val queriesFile=args(4)
                    Hub2(sc, inputPath, outputPath, queriesFile, hubsFile)
                  }
                  case "hub2_new"=>{
                    val hubsFile=args(3)
                    val queriesFile=args(4)
                    Hub2_New(sc, inputPath, outputPath, queriesFile, hubsFile)
                  }

                  case _ => {
                    System.out.println("Wrong parameters!")
                    (0.0, 0.0, 0.0)
                  }
                }

              System.out.println("Loading Graph in " + times._1 + " ms.")
              System.out.println("Finished Running engine in " + times._2 + " ms.")
              System.out.println("Dumping Graph in " + times._3 + " ms.")
            }
          }
