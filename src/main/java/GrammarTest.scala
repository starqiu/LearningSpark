/**
  * Created by qiuxing on 2017/6/20.
  */
class GrammarTest {

}

object GrammarTest extends App{
  val a = Array(3)
  val findMax = (x: Int, y:Int) =>{val m = x max y; println(s"$x $y $m"); m}

  val res = a.scanLeft(0)(_ max _)

  val res1 = a.scanLeft(0)((x,y)=>findMax(x,y))

  println()
}
