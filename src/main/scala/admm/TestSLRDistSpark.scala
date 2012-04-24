package admm

import spark._
import spark.SparkContext
import cern.colt.matrix.tdouble.{DoubleMatrix1D, DoubleFactory1D}
import data.ReutersData._
import admm.Vector._


/**
 * Created by IntelliJ IDEA.
 * User: Jojo
 * Date: 23/04/12
 * Time: 10:03
 * To change this template use File | Settings | File Templates.
 */

object TestSLRDistSpark {

 
  
  def xUpdate(A: SampleSet, b: OutputSet, x: DoubleMatrix1D, u: DoubleMatrix1D, z: DoubleMatrix1D, counter: Int)  {
    x.assign(DoubleFactory1D.dense.make(z.size().toInt, counter))
  }

  def uUpdate(A: SampleSet, b: OutputSet, x: DoubleMatrix1D, u: DoubleMatrix1D, z: DoubleMatrix1D, counter: Int)  {
    u.assign(DoubleFactory1D.dense.make(z.size().toInt, counter))
  }
  
  class mapEnv (sc: SparkContext, filePath: String, topicID: Int, nFeatures: Int, nSlices: Int) extends Serializable {

    val distD = ReutersRDD.localTextRDD(sc, filePath, nFeatures).splitSets(nSlices)

    val x : DoubleMatrix1D =  DoubleFactory1D.dense.make(nFeatures+1)
    val u : DoubleMatrix1D =  DoubleFactory1D.dense.make(nFeatures+1)

    val addXU = distD.map(
    data => (data.generateReutersSet(topicID)._1,data.generateReutersSet(topicID)._2,x,u))

    def setX(doubleMatrix1D: DoubleMatrix1D) {addXU.foreach(dat => dat._3.assign(doubleMatrix1D))}
    def setU(doubleMatrix1D: DoubleMatrix1D) {addXU.foreach(dat => dat._4.assign(doubleMatrix1D))}

    def setXupdated(z: DoubleMatrix1D, counter: Int) {addXU.foreach(dat => xUpdate(dat._1,dat._2,dat._3,dat._4,z,counter))}
    def setUupdated(z: DoubleMatrix1D, counter: Int) {addXU.foreach(dat => uUpdate(dat._1,dat._2,dat._3,dat._4,z,counter))}
  }
  
  def main(args: Array[String]) {
    val sc = new SparkContext("local[2]", "TestSLRDist")
    val nDocs = args(0).toInt
    val nFeatures = args(1).toInt
    val topicID = args(2).toInt
    val nSlices = args(3).toInt
    var lambda = args(4).toDouble
    var rho = args(5).toDouble
    var maxIter = args(6).toInt

    val data =  new mapEnv(sc, "etc/data/labeled_rcv1.admm.data", topicID, nFeatures, nSlices)
    val distData = data.addXU.cache()

    println("Before mutation")
    //distData.foreach(dist => println(dist._2))
    distData.foreach(dist => println(dist._3))
    distData.foreach(dist => println(dist._4))
    
    data.setX(DoubleFactory1D.dense.make(nFeatures+1,10.0))
    data.setU(DoubleFactory1D.dense.make(nFeatures+1,10.0))

    println("After mutation of x and u")
    distData.foreach(dist => println(dist._3))
    distData.foreach(dist => println(dist._4))

    val accumX = sc.accumulator(Vector.zeros(nFeatures+1))
    val accumU = sc.accumulator(Vector.zeros(nFeatures+1))

    val xMean = DoubleFactory1D.dense.make(nFeatures+1)
    val uMean = DoubleFactory1D.dense.make(nFeatures+1)

   for (counter <- 1 to 3) {

    data.setXupdated(DoubleFactory1D.dense.make(nFeatures + 1,1.0), counter)
     distData.foreach {
       data => {
         val x = data._3
         val u = data._4
         accumX += Vector(x.toArray())
         accumU += Vector(u.toArray())
       }
     }

     xMean.assign(accumX.value.elements.map (_ / 2))
     uMean.assign(accumU.value.elements.map (_ / 2))

    data.setUupdated(DoubleFactory1D.dense.make(nFeatures + 1,1.0), counter)

    println("###################################")
    println( "after " + counter + " xUpdated")
    println("###################################")

     println("x")
    distData.foreach(dist => println(dist._3))
     println("u")
     distData.foreach(dist => println(dist._4))
     println("accumX")
    println(accumX.value)
     println("xMean")
     println(xMean)
     println("accumU")
    println(accumU.value)
     println("uMean")
     println(uMean)
   }



  }


}
