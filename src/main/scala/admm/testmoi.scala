package admm

/**
 * Created by IntelliJ IDEA.
 * User: Jojo
 * Date: 19/04/12
 * Time: 01:41
 * To change this template use File | Settings | File Templates.
 */

import scala.Array
import scala.Predef._
import cern.colt.matrix.tdouble.{DoubleFactory2D, DoubleFactory1D}
import cern.jet.math.tdouble.DoubleFunctions

object testMoi {
def main (args: Array[String]) {

  var b = DoubleFactory1D.sparse.random(5)
  var A = DoubleFactory2D.sparse.random(6,5)

 /* var AA = DoubleFactory2D.sparse.diagonal(b).zMult(A,null)

  println("1er")



  println("1er")
  println(b)
  b.reshape(b.size().toInt,1)
  println("2eme")
  println(b) */
  
  println("b")
  println(b)

  b.assign(DoubleFunctions.mult(-1))

  println("after minus")
  println(b)

  b.assign(DoubleFunctions.abs)

  println("after abs")
  println(b)

  b.assign(DoubleFunctions.greater(0.5))
  
  println("after filter")
  println(b)
  
  
}
}
