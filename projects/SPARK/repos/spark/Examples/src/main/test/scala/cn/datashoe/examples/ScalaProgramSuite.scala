package cn.datashoe.examples

import org.scalatest.FunSuite

class ScalaProgramSuite[U] extends FunSuite {
  test("Manifest和ClassTag") {
    import scala.reflect.ClassTag

    // 下面的情况直接用泛型时不行的: 我可能输入两个元素, 两个元素可能同时是string/int, 但我不知道是哪个, 想让它在执行的时候判断
    def arrayMake[T: Manifest](first: T, second: T): Array[T] = {
      val res = new Array[T](2)
      res(0) = first
      res(1) = second
      res
    }

    // 看下array源码对这个的调用
    // ----
    // 这里Array并不知道传入的是什么类型, 但仍然可以加进来, 看下面的源码
    // Array中加入了隐式函数canBuildFrom[T], 内置了一个隐式转换的参数implicit t: ClassTag[T]
    // ClassTag会帮我们存储T的信息
    // 例如：传入1,2根据类型推到可以指定T是Int类型，这时候ClassTag就可以把Int类型这个信息传递给编译器。
    // 也就是说：ClassTag的作用是运行时指定在编译的时候无法指定的类型信息。

    /*
    implicit def canBuildFrom[T](implicit t: ClassTag[T]): CanBuildFrom[Array[_], T, Array[T]] =
      new CanBuildFrom[Array[_], T, Array[T]] {
        def apply(from: Array[_]) = ArrayBuilder.make[T]()(t)
        def apply() = ArrayBuilder.make[T]()(t)
      }
      */

    // 仿照Array写一个类似的例子
    object UDF {
      def apply[T](aa: T)(implicit in: ClassTag[T]): Unit = {
        println("type is: " + in.getClass)
        println("result: " + aa)
        Array[T](aa)
      }
    }

    UDF(1)
    UDF("1")

    // 写一个可以利用array的
    // ---
    // 这样的写法报错: def arrayMake[T](elem: T*): Array[T] = Array(elem:_*)  // No ClassTag available for T
    //    def arrayMake(elem: U*): Array[U] = Array(elem:_*)
    // 但这样可以:
    def mkArray[T: ClassTag](elems: T*) = Array[T](elems: _*)


  }

  test("隐式转换") {
    class A {
      def printA(): Unit = {
        println("打印aa")
      }
    }

    object UseA {
      implicit def implicit2A(): A = new A()
    }

    UseA
  }

}
