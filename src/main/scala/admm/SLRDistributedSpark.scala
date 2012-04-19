package admm

import cern.colt.matrix.tdouble.algo.DenseDoubleAlgebra
import data.RCV1Data._
import cern.jet.math.tdouble.DoubleFunctions
import util.control.Breaks._
import data.SlicedDataSet._
import data.SingleSet._
import data.{SlicedDataSet, SingleSet, DataSet}
import cern.colt.matrix.tdouble.{DoubleFactory1D, DoubleFactory2D, DoubleMatrix1D}
import admmutils.ADMMFunctions
import spark.SparkContext
import SparkContext._


/**
 * Created by IntelliJ IDEA.
 * User: Jojo
 * Date: 19/04/12
 * Time: 02:51
 * To change this template use File | Settings | File Templates.
 */

object SLRDistributedSpark {

  val printStuff = false
  var counter = 0
  type Vector = DoubleMatrix1D
  val algebra = new DenseDoubleAlgebra()
  val alpha = 1000.0

  /*counter+=1
  println("create slice " + counter.toString)*/

  def xUpdate(A: SampleSet, b: OutputSet, x: Vector, u: Vector): (SampleSet, OutputSet, Vector, Vector) = {

    var z = broadcastz.Value

    val bPrime = b.copy()
    bPrime.assign(DoubleFunctions.mult(2.0)).assign(DoubleFunctions.minus(1.0)).assign(DoubleFunctions.mult(alpha))
    val Aprime = DoubleFactory2D.sparse.diagonal(bPrime).zMult(A, null)
    val C = DoubleFactory2D.sparse.appendColumns(bPrime.reshape(bPrime.size().toInt, 1), Aprime)
    //val C = DoubleFactory2D.sparse.appendColumns(bPrime.reshape(bPrime.size().toInt,1),A)
    C.assign(DoubleFunctions.neg)
    val m = A.rows()
    val n = A.columns()

    def loss(x: Vector): Double = {
      val expTerm = C.zMult(x, null)
      expTerm.assign(DoubleFunctions.exp)
        .assign(DoubleFunctions.plus(1.0))
        .assign(DoubleFunctions.log)
      val normTerm = x.copy()
      normTerm.assign(z, DoubleFunctions.minus)
        .assign(u, DoubleFunctions.plus)
      expTerm.zSum() + math.pow(algebra.norm2(normTerm), 2) * rho / 2
    }

    def gradient(x: Vector): Vector = {
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
    def loss(x: Vector): Double = {
      val expTerm = C.zMult(x, null)
      expTerm.assign(DoubleFunctions.exp)
        .assign(DoubleFunctions.plus(1.0))
        .assign(DoubleFunctions.log)
      val normTerm = x.copy()
      normTerm.assign(z, DoubleFunctions.minus)
        .assign(u, DoubleFunctions.plus)
      expTerm.zSum() + math.pow(algebra.norm2(normTerm), 2) * rho / 2
    }
    def backtracking(x: Vector, dx: Vector, grad: Vector): Double = {
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

    def descent(x0: DoubleMatrix1D, maxIter: Int): Vector = {
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
  }


  var maxIter = 100
  var rho = 1.0
  var lambda = 2.0

  /*def solve(dataList : List[(SampleSet,OutputSet)]): DoubleMatrix1D = {
    
        val nDocsPerSlice = dataList.head._1.rows()
        val nFeatures = dataList.head._1.columns()
        val nSlices = dataList.size
        val z: Vector = DoubleFactory1D.dense.make(nFeatures+1)
        val environments = dataList.map{  slice =>
          val x: Vector = DoubleFactory1D.dense.make(nFeatures+1)
          val u: Vector = DoubleFactory1D.dense.make(nFeatures+1)
          MapEnvironment(slice._1, slice._2, x,u,z)
        }
        val xs = environments.map{_.x}
        val us = environments.map{_.u}
        val zUpdate = () => {
          z.assign(ADMMFunctions.mean(xs))
            .assign(ADMMFunctions.mean(us),DoubleFunctions.plus)
            .assign(ADMMFunctions.shrinkage(lambda/rho/nSlices.toDouble))
        }
        for (_ <- 1 to maxIter) {
          environments.foreach{_.xUpdate()}
          zUpdate()
          environments.foreach{_.uUpdate()}
          if (printStuff) {
            println("x update")
            environments.foreach{env => println(algebra.norm2(env.x)) }
            println("z update")
            println(algebra.norm2(z))
            println("u update")
            environments.foreach{env => println(algebra.norm2(env.u))  }
          }
          println(environments.map{env => env.loss(env.x)}.reduce(_+_))
          val xhatdiff= ADMMFunctions.mean(environments.map{_.x})
          println(algebra.norm2(xhatdiff.assign(z, DoubleFunctions.minus)))
          println(z.cardinality())
        }
        z
  }*/


  def solve(data : (SampleSet,OutputSet)): DoubleMatrix1D = {

    val nDocsPerSlice = data._1.rows()
    val nFeatures = data._1.columns()
    val z: Vector = DoubleFactory1D.dense.make(nFeatures+1)

    val x: Vector = DoubleFactory1D.dense.make(nFeatures+1)
    val u: Vector = DoubleFactory1D.dense.make(nFeatures+1)

    val environments = MapEnvironment(data._1, data._2, x,u,z)

    val xs = environments.x
    val us = environments.u
    val zUpdate = () => {
      z.assign(ADMMFunctions.mean(xs))
        .assign(ADMMFunctions.mean(us),DoubleFunctions.plus)
        .assign(ADMMFunctions.shrinkage(lambda/rho/nSlices.toDouble))
    }
    for (_ <- 1 to maxIter) {
      environments.foreach{_.xUpdate()}
      zUpdate()
      environments.foreach{_.uUpdate()}
      if (printStuff) {
        println("x update")
        environments.foreach{env => println(algebra.norm2(env.x)) }
        println("z update")
        println(algebra.norm2(z))
        println("u update")
        environments.foreach{env => println(algebra.norm2(env.u))  }
      }
      println(environments.map{env => env.loss(env.x)}.reduce(_+_))
      val xhatdiff= ADMMFunctions.mean(environments.map{_.x})
      println(algebra.norm2(xhatdiff.assign(z, DoubleFunctions.minus)))
      println(z.cardinality())
    }
    z
  }





  def uUpdate(sample : SampleSet, output: OutputSet, x: Vector, u: Vector) : (SampleSet,OutputSet,Vector,Vector) = {
    var z = broadcastz.Value
    u.assign(x,DoubleFunctions.plus)
      .assign(z,DoubleFunctions.minus)
    (sample,output,x,u)
  }



  
  def addXU (data: (SampleSet,OutputSet), nFeatures : Int) : (SampleSet,OutputSet,Vector,Vector) = {
    
    val x: Vector = DoubleFactory1D.dense.make(nFeatures+1)
    val u: Vector = DoubleFactory1D.dense.make(nFeatures+1)
    
    (data._1,data._2,x,u)
  }











  def main(args: Array[String]) {

    val sc = new SparkContext("local[2]", "SLRDist")

    val nDocs = args(0).toInt
    val nFeatures = args(1).toInt
    val docIndex = args(2).toInt
    val nSlices = args(3).toInt
    lambda = args(4).toDouble
    rho = args(5).toDouble
    maxIter = args(6).toInt
    val dataList = getDatasetList(nDocs,nFeatures,docIndex,nSlices)
    val distData = sc.parallelize(dataList).cache()

    
    val z: Vector = DoubleFactory1D.dense.make(nFeatures+1)
    val broadcastz = sc.broadcast(z)

    distData.foreach {
     data => addXU(data,nFeatures)
    }


    for (_ <- 1 to maxIter) {

      distData.foreach {
        data => xUpdate(data._1, data._2, data._3, data._4)
      }


      val xMean = distData. collect()
      val uMean = distData.

      val zUpdate = () => {
        z.assign(ADMMFunctions.mean(xs))
          .assign(ADMMFunctions.mean(us), DoubleFunctions.plus)
          .assign(ADMMFunctions.shrinkage(lambda / rho / nSlices.toDouble))
      }


      distData.foreach {
        data => uUpdate(data._1, data._2, data._3, data._4)
      }

    }


    val xEst = solve(data)
    val x = xEst.viewPart(1,nFeatures)
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
    }
  }
}
