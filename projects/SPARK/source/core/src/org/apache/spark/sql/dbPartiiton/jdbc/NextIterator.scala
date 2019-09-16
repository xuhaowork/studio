
package org.apache.spark.sql.dbPartiiton.jdbc

abstract class NextIterator[U] extends Iterator[U] {

  private var gotNext = false
  private var nextValue: U = _
  private var closed = false
  protected var finished = false
  protected def getNext(): U
  protected def close()


  def closeIfNeeded() {
    if (!closed) {
      close()
      closed = true
    }
  }

  override def hasNext: Boolean = {
    if (!finished) {
      if (!gotNext) {
        nextValue = getNext()
        if (finished) {
          closeIfNeeded()
        }
        gotNext = true
      }
    }
    !finished
  }

  override def next(): U = {
    if (!hasNext) {
      throw new NoSuchElementException("End of stream")
    }
    gotNext = false
    nextValue
  }
}
