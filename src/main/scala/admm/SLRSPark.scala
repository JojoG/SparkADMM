package admm

import data.ReutersData._
import spark.SparkContext
import admm.SLRSpark.MapEnvironment
import cern.jet.math.tdouble.DoubleFunctions
import admmutils.ADMMFunctions
import util.control.Breaks._
import cern.colt.matrix.tdouble.{DoubleFactory2D, DoubleFactory1D, DoubleMatrix1D, DoubleMatrix2D}
import cern.colt.matrix.tdouble.algo.DenseDoubleAlgebra


/**
 * User: jdr
 * Date: 4/25/12
 * Time: 3:40 PM
 */

object SLRSpark extends App {
  val nSplits = 10
  val splitSize = 10
  val topicIndex = 0
  val sc = new SparkContext("local[2]", "test")
  val algebra = new DenseDoubleAlgebra()

  val rho = 1.0
  val lambda = 0.1
  val alpha = 1

  class MapEnvironment(samples: DoubleMatrix2D, outputs: DoubleMatrix1D) {
    val rho = SLRSpark.rho
    val lambda = SLRSpark.lambda
    val alpha = SLRSpark.alpha
    val n = samples.columns()
    val m = samples.rows()
    val x = DoubleFactory1D.sparse.make(n + 1)
    val u = DoubleFactory1D.sparse.make(n + 1)
    val z = DoubleFactory1D.sparse.make(n + 1)
    val diff = DoubleFactory1D.sparse.make(1)
    

    /*val bPrime = outputs.copy()
    bPrime.assign(DoubleFunctions.mult(2.0)).assign(DoubleFunctions.minus(1.0)).assign(DoubleFunctions.mult(alpha))
    val Aprime = DoubleFactory2D.sparse.diagonal(bPrime).zMult(samples,null)
    val C = DoubleFactory2D.sparse.appendColumns(bPrime.reshape(bPrime.size().toInt,1),Aprime)
    C.assign(DoubleFunctions.neg) */

    def getC : DoubleMatrix2D = {
      val bPrime = outputs.copy()
      bPrime.assign(DoubleFunctions.mult(2.0)).assign(DoubleFunctions.minus(1.0)).assign(DoubleFunctions.mult(alpha))
      val Aprime = DoubleFactory2D.sparse.diagonal(bPrime).zMult(samples,null)
      val C = DoubleFactory2D.sparse.appendColumns(bPrime.reshape(bPrime.size().toInt,1),Aprime)
      C.assign(DoubleFunctions.neg)
      C
    }

    val C = getC

    def updateX {
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
    }

    def updateU {
      u.assign(x, DoubleFunctions.plus).assign(z, DoubleFunctions.minus)
    }

    def updateZ(newZ: DoubleMatrix1D) {
      z.assign(newZ)
    }
    
    def updateDiff {
      val y = DoubleFactory1D.sparse.make(m)
      samples.zMult(z.viewPart(1,n),y)
      y.assign(DoubleFunctions.greater(0.5)).assign(outputs, DoubleFunctions.minus).assign(DoubleFunctions.abs)
      diff.assign(y.zSum()).assign(DoubleFunctions.div(m))
    }
  }

//  val envs = ReutersRDD.hdfsTextRDD(sc, "/user/hduser/data").splitSets(nSplits, splitSize).map(set => {
//    new MapEnvironment(set.samples, set.outputs(topicIndex))
//  }).cache()
  val envs = ReutersRDD.localTextRDD(sc, "etc/data/labeled_rcv1.admm.data",50).splitSets(nSplits).map(set => {
      new MapEnvironment(set.samples, set.outputs(topicIndex))
    }).cache()

  val z = DoubleFactory1D.sparse.make(51)
  for (_ <- 1 to 5) {
    envs.foreach(_.updateX)

    z.assign(
      envs.map(env => {
        val sum = env.x.copy()
        sum.assign(env.u, DoubleFunctions.plus)
        sum
      })
        .reduce(
        (a, b) => {
          a.assign(b, DoubleFunctions.plus)
          a
        })
        .assign(DoubleFunctions.div(nSplits)).assign(ADMMFunctions.shrinkage(lambda / rho / (nSplits.toDouble))))

    envs.foreach(_.z.assign(z))
    envs.foreach(_.updateU)
  }

 val s = DoubleFactory1D.sparse.make(1)
 s.assign(
 envs.map( env => {
   env.updateDiff
   val diff = env.diff.copy()    
   diff
 }   
 ).reduce(
   (a, b) => {
     a.assign(b, DoubleFunctions.plus).assign(DoubleFunctions.div(2))
     a
   })
 )

  println("s")
  println(s)

}