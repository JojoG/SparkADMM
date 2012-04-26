package admm

import admmutils.ADMMFunctions
import cern.jet.math.tdouble.DoubleFunctions
import cern.colt.matrix.tdouble.algo.DenseDoubleAlgebra
import cern.colt.matrix.tdouble.{DoubleFactory2D, DoubleMatrix1D, DoubleFactory1D}
import scala.util.control.Breaks._
import data.{SingleSet, SlicedDataSet, DataSet}
import data.ReutersData.{OutputSet, SampleSet, getBalancedSet, TopicId}
/**
 * User: jdr
 * Date: 4/9/12
 * Time: 9:39 PM
 */

object SLRDistributed {
  val printStuff = false
  var counter = 0
  type Vector = DoubleMatrix1D
  val algebra = new DenseDoubleAlgebra()
  val alpha = 3.0
  case class MapEnvironment(A: SampleSet, b: OutputSet, x: Vector, u: Vector, z: Vector ) {
    counter+=1
    println("create slice " + counter.toString)
    val bPrime = b.copy()
    bPrime.assign(DoubleFunctions.mult(2.0)).assign(DoubleFunctions.minus(1.0)).assign(DoubleFunctions.mult(alpha))
    val Aprime = DoubleFactory2D.sparse.diagonal(bPrime).zMult(A,null)
    val C = DoubleFactory2D.sparse.appendColumns(bPrime.reshape(bPrime.size().toInt,1),Aprime)
    C.assign(DoubleFunctions.neg)
    val m = A.rows()
    val n = A.columns()
    //maybe use another anonymmous variable because x is already in use
    def loss(x: Vector): Double = {
      val expTerm = C.zMult(x,null)
      expTerm.assign(DoubleFunctions.exp)
        .assign(DoubleFunctions.plus(1.0))
        .assign(DoubleFunctions.log)
      val normTerm = x.copy()
      normTerm.assign(z,DoubleFunctions.minus)
        .assign(u,DoubleFunctions.plus)
      expTerm.zSum() + math.pow(algebra.norm2(normTerm),2)*rho/2
    }
    def xUpdate() {
      def gradient(x:Vector): Vector = {
        val expTerm = C.zMult(x,null)
        expTerm.assign(DoubleFunctions.exp)
        val firstTerm = expTerm.copy()
        firstTerm.assign(DoubleFunctions.plus(1.0))
          .assign(DoubleFunctions.inv)
          .assign(expTerm,DoubleFunctions.mult)
        val secondTerm = x.copy()
        secondTerm.assign(z,DoubleFunctions.minus)
          .assign(u,DoubleFunctions.plus)
          .assign(DoubleFunctions.mult(rho))
        val returnValue = C.zMult(firstTerm,null,1.0,1.0,true)
        returnValue.assign(secondTerm,DoubleFunctions.plus)
        returnValue
      }
      def loss(x: Vector): Double = {
        val expTerm = C.zMult(x,null)
        expTerm.assign(DoubleFunctions.exp)
          .assign(DoubleFunctions.plus(1.0))
          .assign(DoubleFunctions.log)
        val normTerm = x.copy()
        normTerm.assign(z,DoubleFunctions.minus)
          .assign(u,DoubleFunctions.plus)
        expTerm.zSum() + math.pow(algebra.norm2(normTerm),2)*rho/2
      }
      def backtracking(x: Vector, dx: Vector, grad: Vector): Double = {
        val t0 = 1.0
        val alpha = .1
        val beta = .5
        val lossX = loss(x)
        val rhsCacheTerm = dx.zDotProduct(grad)*alpha
        def lhs(t: Double): Double = {
          val newX = x.copy()
          newX.assign(dx,DoubleFunctions.plusMultSecond(t))
          loss(newX)
        }
        def rhs(t: Double): Double = {
          lossX + t*rhsCacheTerm
        }
        def helper(t: Double): Double = {
          if (lhs(t) > rhs(t)) helper(beta*t) else t
        }
        helper(t0)
      }

      def descent(x0: DoubleMatrix1D, maxIter: Int): Vector = {
        val tol = 1e-4
        breakable {for (i <- 1 to maxIter) {
          val dx = gradient(x0)
          dx.assign(DoubleFunctions.neg)
          val t = backtracking(x,dx,gradient(x0))
          x0.assign(dx,DoubleFunctions.plusMultSecond(t))
          if (algebra.norm2(dx) < tol) break()
        }
        }
        x0
      }
      x.assign(descent(x,25))
    }
    def uUpdate() {
      u.assign(x,DoubleFunctions.plus)
        .assign(z,DoubleFunctions.minus)
    }
  }
  var maxIter = 100
  var rho = 1.0
  var lambda = 2.0

  def solve(data: DataSet[SampleSet,OutputSet]): DoubleMatrix1D = {
    data match {
      case SingleSet(samples,output) => {
        solve(SlicedDataSet(List(SingleSet(samples,output))))//don't understand
      }
      case SlicedDataSet(slices) => {
        val nDocsPerSlice = slices.head.samples.rows
        val nFeatures = slices.head.samples.columns
        val nSlices = slices.size
        val z: Vector = DoubleFactory1D.dense.make(nFeatures+1)
        val environments = slices.map{  slice =>
          val x: Vector = DoubleFactory1D.dense.make(nFeatures+1)
          val u: Vector = DoubleFactory1D.dense.make(nFeatures+1)
          MapEnvironment(slice.samples, slice.output, x,u,z)
        }
        val xs = environments.map{_.x}
        val us = environments.map{_.u}
        val zUpdate = () => {
          z.assign(ADMMFunctions.mean(xs))
            .assign(ADMMFunctions.mean(us),DoubleFunctions.plus)         //or minus??
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
    }
  }
  def main(args: Array[String]) {
    val nDocs = args(0).toInt
    val nFeatures = args(1).toInt
    val docIndex: TopicId = args(2).toInt
    val nSlices = args(3).toInt
    val proportion = .5
    lambda = args(4).toDouble
    rho = args(5).toDouble
    maxIter = args(6).toInt
    val slicedSet = getBalancedSet(nDocs, nFeatures, docIndex, nSlices, proportion)
    val xEst = solve(slicedSet)
    val x = xEst.viewPart(1,nFeatures)
    val goodslices = slicedSet match {
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
    println("got good")
    val badSlices = slicedSet match {
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
    println("got good")
    val vwish = -.5*(goodslices.reduce{_+_}/goodslices.size + badSlices.reduce{_+_}/badSlices.size)
    val vreal = xEst.getQuick(0)
    val vs = List(vreal,vwish)
    println("got good")
    vs.map{v =>{
      println("got good vs")
      def loss(mu: Double): Double = math.log(1 + math.exp(-mu))
      def mu(ai: DoubleMatrix1D, bi: Double): Double = (2*bi - 1)*(ai.zDotProduct(x) + v)
      val totalLoss = slicedSet match {
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


    slicedSet match {
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


