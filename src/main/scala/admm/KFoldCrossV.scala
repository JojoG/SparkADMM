package admm

import cern.jet.math.tdouble.DoubleFunctions
import cern.colt.matrix.tdouble.{DoubleFactory2D, DoubleFactory1D, DoubleMatrix1D, DoubleMatrix2D}
import util.control.Breaks._
import spark.SparkContext
import cern.colt.matrix.tdouble.algo.DenseDoubleAlgebra
import data.ReutersData.ReutersRDD
import admmutils.ADMMFunctions

/**
 * Created by IntelliJ IDEA.
 * User: Jojo
 * Date: 30/04/12
 * Time: 23:04
 * To change this template use File | Settings | File Templates.
 */

object KFoldCrossV extends App {

  val K = 10
  //val nSplits = 10
  //val splitSize = 10
  val topicIndex = 0
  val nFeatures = 50
  val sc = new SparkContext("local[2]", "test")
  val algebra = new DenseDoubleAlgebra()

  var kTot = 0

  val rho = 1.0
  val lambda = 0.1
  val alpha = 1

  class MapEnvironment(samples: DoubleMatrix2D, outputs: DoubleMatrix1D) {
    val rho = KFoldCrossV.rho
    val lambda = KFoldCrossV.lambda
    val alpha = KFoldCrossV.alpha
    val n = samples.columns()
    val m = samples.rows()
    val x = DoubleFactory1D.sparse.make(n + 1)
    val u = DoubleFactory1D.sparse.make(n + 1)
    val z = DoubleFactory1D.sparse.make(n + 1)
    val diff = DoubleFactory1D.sparse.make(1)
    kTot += 1
    val k = kTot

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
      y.assign(DoubleFunctions.greater(0)).assign(outputs, DoubleFunctions.minus).assign(DoubleFunctions.abs)
      diff.assign(y.zSum()).assign(DoubleFunctions.div(m))
    }
  }

  val envs = ReutersRDD.localTextRDD(sc, "etc/data/labeled_rcv1.admm.data",nFeatures).splitSets(K).map(set => {
    new MapEnvironment(set.samples, set.outputs(topicIndex))
  }).cache()


  val s = DoubleFactory1D.sparse.make(1)

  for (i <- 1 to K) {

    val z = DoubleFactory1D.sparse.make(nFeatures + 1)
    envs.foreach{env => {
      env.x.assign(DoubleFactory1D.sparse.make(nFeatures + 1))
      env.z.assign(DoubleFactory1D.sparse.make(nFeatures + 1))
      env.u.assign(DoubleFactory1D.sparse.make(nFeatures + 1))
    }}

    
    for (_ <- 1 to 5) {
      envs.foreach( env => {
        if( env.k.==(i) == false ){
          env.updateX;
        }
      })

      z.assign(
        envs.map(env => {
          if( env.k.==(i) == false ){  
            val sum = env.x.copy()
            sum.assign(env.u, DoubleFunctions.plus)
            sum
          } else {DoubleFactory1D.sparse.make(nFeatures + 1)}
        })
          .reduce(
          (a, b) => {
            a.assign(b, DoubleFunctions.plus)
            a
          })
          .assign(DoubleFunctions.div(K-1)).assign(ADMMFunctions.shrinkage(lambda / rho / ((K-1).toDouble))))

      envs.foreach(env => {
        if( env.k.!=(i) ){
          env.z.assign(z)
        }
      })
      envs.foreach(env => {
        if( env.k.!=(i) ){
          env.updateU
        }
      })
    }


    s.assign(
      envs.map( env => {
        if( env.k.==(i) ){
          env.z.assign(z)
          env.updateDiff
          val diff = env.diff.copy()
          diff
        }  else {DoubleFactory1D.sparse.make(1)}
      }
      ).reduce(
        (a, b) => {
          a.assign(b, DoubleFunctions.plus)
          a
        })
    )
  }
  
  s.assign(DoubleFunctions.div(K))
  
  println("s")
  println(s)
}
