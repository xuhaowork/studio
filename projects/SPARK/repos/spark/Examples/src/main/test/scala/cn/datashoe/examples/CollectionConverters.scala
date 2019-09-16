package cn.datashoe.examples

import org.scalatest.FunSuite


/**
  * java集合类和scala集合类的相互转换
  */
class CollectionConverters extends FunSuite {
  /**
    * java集合类和scala集合类的相互转换
    * ----
    * DecorateAsJava
    */
  test("JavaConverters类") {
    import scala.collection.JavaConverters._
    /**
      * 转化规则
      */
    /**
      * asJava
      * ----
      * `asJavaCollection`, `asJavaEnumeration`, `asJavaDictionary`
      * ----
      * `scala.collection.Iterable` <=> `java.lang.Iterable`
      * `scala.collection.Iterator` <=> `java.util.Iterator`
      * `scala.collection.mutable.Buffer` <=> `java.util.List`
      * `scala.collection.mutable.Set` <=> `java.util.Set`
      * `scala.collection.mutable.Map` <=> `java.util.Map`
      * `scala.collection.mutable.concurrent.Map` <=> `java.util.concurrent.ConcurrentMap`
      * `scala.collection.Seq` => `java.util.List`
      * `scala.collection.mutable.Seq` => `java.util.List`
      * `scala.collection.Set` => `java.util.Set`
      * `scala.collection.Map` => `java.util.Map`
      * --
      * `scala.collection.Iterable` <=> `java.util.Collection`
      * `scala.collection.Iterator` <=> `java.util.Enumeration`
      * `scala.collection.mutable.Map` <=> `java.util.Dictionary`
      */

    val input: Iterator[Int] = null
    input.asJava

  }

}
