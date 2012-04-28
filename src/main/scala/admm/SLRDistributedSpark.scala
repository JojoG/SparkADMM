package admm

import cern.colt.matrix.tdouble.algo.DenseDoubleAlgebra

import cern.jet.math.tdouble.DoubleFunctions
import util.control.Breaks._
import spark.SparkContext
import data.ReutersData._
import cern.colt.matrix.tdouble.{DoubleMatrix1D, DoubleFactory1D, DoubleFactory2D}
import spark.{RDD, SparkContext}
import admm.SLRDistributedSpark.mapEnv
import admm.Vector._
import admmutils.ADMMFunctions


/**
 * Created by IntelliJ IDEA.
 * User: Jojo
 * Date: 19/04/12
 * Time: 02:51
 * To change this template use File | Settings | File Templates.
 */

object SLRDistributedSpark {

  def xUpdate(A: SampleSet, b: OutputSet, x: DoubleMatrix1D, u: DoubleMatrix1D, z: DoubleMatrix1D ) {
    val bPrime = b.copy()
    bPrime.assign(DoubleFunctions.mult(2.0)).assign(DoubleFunctions.minus(1.0)).assign(DoubleFunctions.mult(alpha))
    val Aprime = DoubleFactory2D.sparse.diagonal(bPrime).zMult(A, null)
    val C = DoubleFactory2D.sparse.appendColumns(bPrime.reshape(bPrime.size().toInt, 1), Aprime)
    C.assign(DoubleFunctions.neg)

    def loss(x: DoubleMatrix1D): Double = {
      val expTerm = C.zMult(x, null)
      expTerm.assign(DoubleFunctions.exp)
        .assign(DoubleFunctions.plus(1.0))
        .assign(DoubleFunctions.log)
      val normTerm = x.copy()
      normTerm.assign(z, DoubleFunctions.minus)
        .assign(u, DoubleFunctions.plus)
      expTerm.zSum() + math.pow(algebra.norm2(normTerm), 2) * rho / 2
    }

    def gradient(x: DoubleMatrix1D): DoubleMatrix1D = {
      val expTerm = C.zMult(x, null)
      expTerm.assign(DoubleFunctions.exp)
      val firstTerm = expTerm.copy()
      firstTerm.assign(DoubleFunctions.plus(1.0))
        .assign(DoubleFunctions.inv)
        .assign(expTerm, DoubleFunctions.mult)
      val secondTerm = x.copy()
      secondTerm.assign(z, DoubleFunctions.minus)
        .assign(u, DoubleFunctions.plus)
        .assign(DoubleFunctions.mult(rho))
      val returnValue = C.zMult(firstTerm, null, 1.0, 1.0, true)
      returnValue.assign(secondTerm, DoubleFunctions.plus)
      returnValue
    }
    def backtracking(x: DoubleMatrix1D, dx: DoubleMatrix1D, grad: DoubleMatrix1D): Double = {
      val t0 = 1.0
      val alpha = .1
      val beta = .5
      val lossX = loss(x)
      val rhsCacheTerm = dx.zDotProduct(grad) * alpha
      def lhs(t: Double): Double = {
        val newX = x.copy()
        newX.assign(dx, DoubleFunctions.plusMultSecond(t))
        loss(newX)
      }
      def rhs(t: Double): Double = {
        lossX + t * rhsCacheTerm
      }
      def helper(t: Double): Double = {
        if (lhs(t) > rhs(t)) helper(beta * t) else t
      }
      helper(t0)
    }

    def descent(x0: DoubleMatrix1D, maxIter: Int): DoubleMatrix1D = {
      val tol = 1e-4
      breakable {
        for (i <- 1 to maxIter) {
          val dx = gradient(x0)
          dx.assign(DoubleFunctions.neg)
          val t = backtracking(x, dx, gradient(x0))
          x0.assign(dx, DoubleFunctions.plusMultSecond(t))
          if (algebra.norm2(dx) < tol) break()
        }
      }
      x0
    }
    x.assign(descent(x, 25))
    println("x intern after upgraded")
    println(x)
  }

  def uUpdate(sample : SampleSet, output: OutputSet, x: DoubleMatrix1D, u: DoubleMatrix1D, z: DoubleMatrix1D ) {
    u.assign(x,DoubleFunctions.plus)
      .assign(z,DoubleFunctions.minus)
    //println("u intern after upgraded")
    //println(u)
  }

  class mapEnv (sc: SparkContext, filePath: String, topicID: Int, nFeatures: Int, nSlices: Int) extends Serializable {

    val distD = ReutersRDD.localTextRDD(sc, filePath, nFeatures).splitSets(nSlices)

    //val x : DoubleMatrix1D =  DoubleFactory1D.dense.make(nFeatures+1)
    //val u : DoubleMatrix1D =  DoubleFactory1D.dense.make(nFeatures+1)
    //val accX : DoubleMatrix1D =  DoubleFactory1D.dense.make(nFeatures+1)
    //val accU : DoubleMatrix1D =  DoubleFactory1D.dense.make(nFeatures+1)

    val addXU = distD.map( data => {
      val set = data.generateReutersSet(topicID)
      val A = set._1
      val b = set._2
      (A,b,DoubleFactory1D.dense.make(nFeatures+1),DoubleFactory1D.dense.make(nFeatures+1),DoubleFactory1D.dense.make(nFeatures+1))
    })

    def setX(doubleMatrix1D: DoubleMatrix1D) {addXU.foreach(dat => dat._3.assign(doubleMatrix1D))}
    def setU(doubleMatrix1D: DoubleMatrix1D) {addXU.foreach(dat => dat._4.assign(doubleMatrix1D))}

   // def getA {addXU.foreach(dat => println(dat._1))}
   // def getB {addXU.foreach(dat => println(dat._2))}

   // def getX {addXU.foreach(dat => println(dat._3))}
   // def getU {addXU.foreach(dat => println(dat._4))}

    def setXupdated(z: DoubleMatrix1D) {addXU.foreach(dat => xUpdate(dat._1,dat._2,dat._3,dat._4, z))}
    def setUupdated(z: DoubleMatrix1D) {addXU.foreach(dat => uUpdate(dat._1,dat._2,dat._3,dat._4,z))}
  }

  val algebra = new DenseDoubleAlgebra()
  val alpha = 3.0
  var maxIter = 100
  var rho = 1.0
  var lambda = 2.0

  def main(args: Array[String]) {

    val sc = new SparkContext("local", "SLRDist")
    val nDocs = args(0).toInt
    val nFeatures = args(1).toInt
    val topicID = args(2).toInt
    val nSlices = args(3).toInt
    lambda = args(4).toDouble
    rho = args(5).toDouble
    maxIter = args(6).toInt

    val data =  new mapEnv(sc, "etc/data/labeled_rcv1.admm.data", topicID, nFeatures, nSlices)
    val distDataXU = data.addXU.cache()

    val z: DoubleMatrix1D = DoubleFactory1D.dense.make(nFeatures+1)

    val xMean = DoubleFactory1D.dense.make(nFeatures+1)
    val uMean = DoubleFactory1D.dense.make(nFeatures+1)


    for (_ <- 1 to maxIter) {
      val accumX = sc.accumulator(Vector.zeros(nFeatures+1))
      val accumU = sc.accumulator(Vector.zeros(nFeatures+1))

      distDataXU.foreach{ data => {
          val A = data._1
          val b = data._2
          val x = data._3 
          val u = data._4
          xUpdate(A,b,x,u,z)
          accumX += Vector(x.toArray())
          accumU += Vector(u.toArray())
          //println("accX in the loop")
         // println(accX)
          }
      }

      //println("xmean before updates")
      //println(xMean)
      //println("umean before updates")
      //println(uMean)
      
      
      xMean.assign(accumX.value.elements.map (_ / nSlices))
      //println("xmean")
      //println(xMean)
      //println("cardinal xmean")
      //println(xMean.cardinality())
      //println("###############################accumX after updates##############################################")
      //println(accumX)

      uMean.assign(accumU.value.elements.map (_ / nSlices))
     // println("umean")
     // println(uMean)

      z.assign(xMean,DoubleFunctions.plus).assign(uMean,DoubleFunctions.plus).assign(ADMMFunctions.shrinkage(lambda/rho/nSlices.toDouble))

      //println("z")
     // println(z.cardinality())
      
      data.setUupdated(z)

      xMean.assign(DoubleFactory1D.dense.make(nFeatures+1))
      uMean.assign (DoubleFactory1D.dense.make(nFeatures+1))

    }

    //println(z.assign(DoubleFunctions.abs).assign(DoubleFunctions.isLess(0.00001),0).cardinality())
    println("cardinality z")
    println(z.viewPart(1,nFeatures).cardinality())

    // setXupdated works...
    /*val data1 =  new mapEnv(sc, "etc/data/labeled_rcv1.admm.data", topicID, nFeatures, nSlices)
    val distDataXU1 = data1.addXU.cache()
    val z1 = DoubleFactory1D.dense.make(nFeatures+1)
    println("From the outside")
    //distDataXU1.foreach(dat => xUpdate(dat._1, dat._2, dat._3, dat._4, z1 ))
    distDataXU1.foreach(dat => uUpdate(dat._1, dat._2, dat._3, dat._4, z1 ))
    val data2 =  new mapEnv(sc, "etc/data/labeled_rcv1.admm.data", topicID, nFeatures, nSlices)
    //val distDataXU2 = data2.addXU.cache()
    val z2 = DoubleFactory1D.dense.make(nFeatures+1)
    println("From inside")
    //data2.setXupdated(z2)
    data2.setUupdated(z2)*/

   /*val x = z.viewPart(1,nFeatures)
    val goodslices = data match {
      case SlicedDataSet(slices) => {
        slices.map{
          case SingleSet(a,b) => {
            a.toArray.zip(b.toArray).filter{
              case (ai,bi) => bi > .5
            }.map{case (ai,bi) => DoubleFactory1D.sparse.make(ai).zDotProduct(x)}
          }
        }.flatten
      }
    }
    val badSlices = data match {
      case SlicedDataSet(slices) => {
        slices.map{
          case SingleSet(a,b) => {
            a.toArray.zip(b.toArray).filter{
              case (ai,bi) => bi < .5
            }.map{case (ai,bi) => DoubleFactory1D.sparse.make(ai).zDotProduct(x)}
          }
        }.flatten
      }
    }
    val vwish = -.5*(goodslices.reduce{_+_}/goodslices.size + badSlices.reduce{_+_}/badSlices.size)
    val vreal = xEst.getQuick(0)
    val vs = List(vreal,vwish)
    vs.map{v =>{
      def loss(mu: Double): Double = math.log(1 + math.exp(-mu))
      def mu(ai: DoubleMatrix1D, bi: Double): Double = (2*bi - 1)*(ai.zDotProduct(x) + v)
      val totalLoss = data match {
        case SlicedDataSet(slices) => {
          slices.map{
            case SingleSet(as,bs) => {
              as.toArray.zip(bs.toArray).map{case (ai,bi) =>{
                loss(mu(DoubleFactory1D.dense.make(ai),bi))
              }}
            }
          }.flatten.reduce{_+_}
        }
      }
      println(totalLoss)
    }}
    val goodavg = goodslices.reduce{_+_}/goodslices.size
    val badavg = badSlices.reduce{_+_}/badSlices.size
    val v = vreal


    data match {
      case SlicedDataSet(slices) => {
        var onegood = 0
        var onetotal = 0
        var zerogood = 0
        var zerototal = 0
        slices.foreach{slice =>{
          val A = slice.samples
          val b = slice.output

          (0 until A.rows()).map{A.viewRow(_)}.zip(b.toArray).foreach{case (ai,bi) =>{
            val biprime = bi*2 - 1
            val mu = biprime*(x.zDotProduct(ai) + v)
            bi match {
              case 0 => {zerototal+=1}
              case 1 => {onetotal+=1}
            }
            mu > 0 match {
              case true => {
                bi match {
                  case 0 => {zerogood+=1}
                  case 1 => {onegood+=1}
                }
              }
              case _ => {}
            }
          }
          }}

        }
        println("positive success: " + (onegood.toDouble/onetotal).toString)
        println("negative success: " + (zerogood.toDouble/zerototal).toString)
        println(v)
        println(goodavg )
        println(badavg)
        println(goodslices.map{a => math.pow(a-goodavg,2)}.reduce{_+_}/goodslices.size)
        println(badSlices.map{a => math.pow(a-badavg,2)}.reduce{_+_}/badSlices.size)
        println(goodslices.size.toDouble/(goodslices.size + badSlices.size))
      }
    } */
  }
}
