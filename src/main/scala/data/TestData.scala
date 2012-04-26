package data

import cern.jet.math.tdouble.DoubleFunctions
import admmutils.ADMMFunctions
import util.Random
import cern.colt.matrix.tdouble.{DoubleFactory1D, DoubleMatrix1D, DoubleMatrix2D, DoubleFactory2D}
import admm.SLRDistributed._
import admm.SLRDistributed.solve
import data.SingleSet
import admm.SLRDistributed

/**
 * User: jdr
 * Date: 4/10/12
 * Time: 2:31 PM
 */

object TestData {
  case class DataSetWithTruth[A,B,T<:DataSet[A,B]](truth: B, noise: T)
  def slrData(m: Int, n: Int, sparsity: Double): DataSetWithTruth[DoubleMatrix2D, DoubleMatrix1D, SingleSet[DoubleMatrix2D,DoubleMatrix1D]] =  {
    val w = ADMMFunctions.sprandnvec(n,sparsity)
    val v = Random.nextGaussian()
    val trueX = DoubleFactory1D.sparse.append(DoubleFactory1D.sparse.make(1,v),w)
    val X = ADMMFunctions.sprandnMatrix(m,n, sparsity)
    val bTrue = X.zMult(w,null)
    bTrue.assign(DoubleFunctions.plus(v))
      .assign(DoubleFunctions.sign)
    //Calculation of A
    val bNoise = bTrue.copy()
    val noise = ADMMFunctions.sprandnvec(m,1.0)
    noise.assign(DoubleFunctions.mult(.01))
    bNoise.assign(noise,DoubleFunctions.plus)
    val B = DoubleFactory2D.sparse.diagonal(bNoise)//
    val A = DoubleFactory2D.sparse.make(m,n)//don't understand
    B.zMult(X,A)//for me what we want is more
    //DataSetWithTruth(trueX,SingleSet(X,bNoise))    (cf Boyd)
    val out: DataSetWithTruth[DoubleMatrix2D, DoubleMatrix1D,SingleSet[DoubleMatrix2D,DoubleMatrix1D]] = DataSetWithTruth(trueX,SingleSet(A,bNoise))
    out
  }
  /*def main(args: Array[String]) {
    val m = args(0).toInt //nbDocs
    val n = args(1).toInt //nbFeatures
    val sparsityA = args(2).toInt //sparsity in lines of feature matrix A
    val sparsityW = args(3).toInt //sparsity in w the true feature vector parameter
    val lambda = args(4).toDouble
    val rho = args(5).toDouble
    val proportion = args(6).toDouble //proportion of positive results (rare events)
    /*
    for now we don't use termination criteria, just do the max iterations
     */
    val maxIter = args(7).toInt //for now
    val data = slrData(m,n,sparsityA)//should add sparsityW

    SLRDistributed.lambda//how do I do if I want to set lambda and not use the lambda that is already
    // in SLR Distributed? Same question for rho, proportion, maxIter...

    val singleSet = data.T
    val xEst = solve(singleSet)



    /*case class SingleSet[A,B](samples: A, output: B) extends DataSet[A,B]
    
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
    }*/

  }*/
}
