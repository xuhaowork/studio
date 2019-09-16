package cn.datashoe

package object BLAS {
  import breeze.linalg.{DenseMatrix => BDM, DenseVector => BDV}
  import breeze.util.JavaArrayOps
  import com.github.fommil.netlib.ARPACK
  import org.netlib.util.{doubleW, intW}

  class MatrixOpsWithARPACK {

  }

  object MatrixOpsWithARPACK {
    // http://www.netlib.org/lapack/explore-html/index.html



    // 使用ARPACK算法实现特征向量分解
    // 模式1只需要乘, 模式3需要方程组求解
    def eigen(mul: BDV[Double] => BDV[Double], n: Int, k: Int, tol: Double = 1e-6, which: String = "LM", mode: Int = 3, maxIterations: Int = 100, ptds: Int = 2, sigma: Double = 0.0): Array[Array[Double]] = {

      val arpack = ARPACK.getInstance()
      val tolW = new doubleW(tol)
      //val nev = new intW(k + 1)
      val nev = new intW(k)
      //val ncv = if (mode == 3) math.max(2 * (k + 1), 20) else math.min(3 * k, n)
      val ncv = if (mode == 3) math.min(math.max(2 * k + 1, 20), n) else math.min(3 * k, n)
      val bmat = "I"
      var iparam = new Array[Int](11)
      iparam(0) = 1
      iparam(2) = maxIterations
      if (mode == 3) iparam(3) = 1
      iparam(6) = mode

      val ido = new intW(0)
      val info = new intW(0)
      val resid = new Array[Double](n)

      val v = new Array[Double](n * ncv)
      val workd = new Array[Double](n * 3)
      val workl = new Array[Double](ncv * (ncv + 8))
      val ipntr = new Array[Int](11)

      arpack.dsaupd(ido, bmat, n, which, nev.`val`, tolW, resid, ncv, v, n, iparam, ipntr, workd,
        workl, workl.length, info)

      val w = BDV(workd)

      while (ido.`val` != 99) {
        val inputOffset = ipntr(0) - 1
        val outputOffset = ipntr(1) - 1
        val x = w.slice(inputOffset, inputOffset + n)
        val y = w.slice(outputOffset, outputOffset + n)

        ido.`val` match {
          case -1 =>
            y := mul(x)
          case 1 =>
            if (mode == 1) {
              y := mul(x)
            } else {
              val Bxslice = w.slice(ipntr(2) - 1, ipntr(2) - 1 + n)
              y := mul(Bxslice)
            }
          case _ =>
            println("ARPACK 执行失败, 无法找到合适的特征向量")
        }
        arpack.dsaupd(ido, bmat, n, which, nev.`val`, tolW, resid, ncv, v, n, iparam, ipntr,
          workd, workl, workl.length, info)
      }

      if (info.`val` != 0) {
        println("达到最大迭代次数, ARPACK算法无法收敛, 请检查输入数据是否满足算法要求")
      }

      val d = new Array[Double](nev.`val`)
      val select = new Array[Boolean](ncv)
      val z = java.util.Arrays.copyOfRange(v, 0, nev.`val` * n)

      arpack.dseupd(true, "A", select, d, z, n, sigma, bmat, n, which, nev, tolW.`val`, resid, ncv, v, n,
        iparam, ipntr, workd, workl, workl.length, info)

      JavaArrayOps.dmDToArray2(new BDM[Double](n, nev.`val`, z)(::,
        if (mode == 3)
        //1 until nev.`val`
          0 until nev.`val`
        else
          (nev.`val` - ptds - 1) to -2))
    }


  }





  import breeze.linalg.{CSCMatrix, DenseMatrix, DenseVector, Matrix}

  import scala.collection.mutable.ArrayBuffer

  class MatrixOpsWithBreeze {

  }

  object MatrixOpsWithBreeze {

    // 统计矩阵中的零值
    def zeroCnt(mat: Matrix[Double]): Long = {
      var cnt = 0l
      val n = mat.rows
      for (i <- 0 until n) {
        for (j <- 0 until i) {
          if (mat(i, j) == 0.0) cnt += 1
        }
      }

      cnt
    }

    // 打印稀疏矩阵
    def printMat(mat: CSCMatrix[Double]): Unit = {
      for (i <- 0 until mat.cols) {
        for (j <- mat.colPtrs(i) until mat.colPtrs(i + 1)) {
          print(f"($i%3d, ${mat.rowIndices(j)}%3d) = ${mat.data(j)}% .3f , ")
        }
        println()
      }
    }

    // 打印稠密矩阵
    def printMat(mat: DenseMatrix[Double], noZero: Boolean = true): Unit = {
      if (noZero) {
        print("       ")
        for (j <- 0 until mat.cols) {
          print(f"$j%4d   ")
        }
        println()
      }

      for (i <- 0 until mat.rows) {
        if (noZero) {
          print(f"$i%4d ")
        }

        for (j <- 0 until mat.cols) {
          if (noZero) {
            print(f"${mat(i, j)}% .3f ")
          } else {
            if (mat(i, j) != 0) {
              print(f"($i%3d, $j%3d) = ${mat(i, j)}% .3f , ")
            }
          }
        }
        println()
      }
    }

    // 显示矩阵
    def showMat(mat: Matrix[Double]): Unit = {
      val data = ArrayBuffer[(Int, Int)]()
      for (i <- 0 until mat.rows) {
        for (j <- 0 until mat.cols) {
          if (mat(i, j) != 0.0) {
            data.append((i, j))
          }
        }
      }

      //VegasUtils.scatterWithXY(data.toArray, mark = vegas.spec.Spec.MarkEnums.Square)
    }

    // 计算稀疏矩阵转置后与向量相乘
    def mtx(m: CSCMatrix[Double], v: DenseVector[Double], out: DenseVector[Double]): Unit = {
      for (col <- 0 until m.cols) {
        for (row <- m.colPtrs(col) until m.colPtrs(col + 1)) {
          out(col) += m.data(row) * v.unsafeValueAt(m.rowIndices(row))
        }
      }
    }

    // 返回指定列的行索引
    def getRowsByColIndex(mat: CSCMatrix[Double], colIndex: Int): Range = {
      mat.colPtrs(colIndex) until mat.colPtrs(colIndex + 1)
    }

    // 自己实现的 Cholesky 分解的变种, LDLt分解
    // 如果mat是稠密矩阵, 会直接使用该矩阵的内存, 会改变mat里的值
    // 如果mat是稀疏矩阵, 就将下三角矩阵转为稠密矩阵
    // 返回 L DLt, 都是稠密矩阵
    def cholesky(mat: Matrix[Double]): (DenseMatrix[Double], DenseMatrix[Double]) = {
      val dm = mat match {
        case m: DenseMatrix[Double] => m
        case _ => mat.asInstanceOf[CSCMatrix[Double]].toDense
        // 使用下面的方式会慢非常多, 好奇怪
        //case _ => lowerTriangular(mat)
      }

      val n = mat.rows
      for (k <- 0 until n) {
        for (i <- 0 until k) {
          val mki = dm.unsafeValueAt(k, i)
          val mii = dm.unsafeValueAt(i, i)
          val mkk = dm.unsafeValueAt(k, k)
          dm.unsafeUpdate(k, k, mkk - mii * mki * mki)
        }

        // 因为计算时取的值都是已经被计算过的, 几乎无法利用矩阵的稀疏性
        for (i <- 0 until k) {
          val mki = dm.unsafeValueAt(k, i)
          val mii = dm.unsafeValueAt(i, i)
          val mkimii = mki * mii

          for (j <- k + 1 until n) {
            val mji = dm.unsafeValueAt(j, i)
            val mjk = dm.unsafeValueAt(j, k)
            dm.unsafeUpdate(j, k, mjk - mji * mkimii)
          }
        }
        val mkk = dm(k, k)
        for (j <- k + 1 until n) {
          val mjk = dm.unsafeValueAt(j, k)
          dm.unsafeUpdate(j, k, mjk / mkk)
        }
      }

      /*
      // 分解稀疏矩阵, 已废弃
      for (k <- 0 until n) {
          //for (k <- 0 until 1) {
          //    for (j <- M.colPtrs(k) until M.colPtrs(k + 1)) {
          //        val i = M.rowIndices(j)
          //        println(s"i: $i ${M(k, k)} ${M(i, i)} ${M.data(j)}")
          //        M(k, k) -= M(i, i) * M.data(j) * M.data(j)
          //        println(s"${M(k, k)}")
          //    }
          for (i <- 0 until k) {
              mat(k, k) -= mat(i, i) * mat(k, i) * mat(k, i)
          }

          //// 非判断方式, 会慢一点点
          //for (i <- 0 until k) {
          //    val rows = getRowsByColIndex(mat, i)
          //    for (row <- rows) {
          //        val j = mat.rowIndices(row)
          //        if (j > k) {
          //            mat(j, k) -= mat.data(row) * mat(i,i) * mat(k,i)
          //        }
          //    }
          //}

          // 确定第i列里的第j,i,k行同时存在数据
          for (i <- 0 until k) {
              val mki = mat(k, i)
              if (mki != 0.0) {
                  val mii = mat(i, i)
                  if (mii != 0.0) {
                      val rows = getRowsByColIndex(mat, i)
                      for (row <- rows) {
                          val j = mat.rowIndices(row)
                          if (j > k) {
                              mat(j, k) -= mat.data(row) * mii * mki
                          }
                      }
                  }
              }
          }

          // 直接计算, 会稍稍慢一点
          //for (i <- 0 until k) {
          //    for (j <- k + 1 until n) {
          //        M(j, k) -= M(j, i) * M(i, i) * M(k, i)
          //    }
          //}
          for (j <- k + 1 until n) {
              mat(j, k) /= mat(k, k)
          }
      }
      */
      // 计算D, L
      val D = CSCMatrix.zeros[Double](n, n)

      val L = dm
      for (i <- 0 until n) {
        D(i, i) = L(i, i)
        L(i, i) = 1
      }

      val Lt = L.t
      val DLt = D * Lt

      (L, DLt)
    }

    // 传入 L DLt, 解方程
    def solve(L: Matrix[Double], DLt: Matrix[Double], b: DenseVector[Double]): DenseVector[Double] = {
      val x = b.copy
      val n = b.length
      for (k <- 0 until n) {
        for (i <- 0 until k) {
          x(k) -= x(i) * L(k, i)
        }
        x(k) /= L(k, k)
      }

      for (k <- n - 1 to 0 by -1) {
        for (i <- k + 1 until n) {
          x(k) -= x(i) * DLt(k, i)
        }
        x(k) /= DLt(k, k)
      }
      x
    }
  }





  import breeze.linalg.{CSCMatrix, DenseMatrix => BDM, DenseVector => BDV, Matrix => BM}
  import com.github.fommil.netlib.LAPACK.{getInstance => lapack}
  import org.netlib.util.intW

  class MatrixOpsWithLAPACK {

  }

  object MatrixOpsWithLAPACK {

    // LU 分解 X = P * L * U
    // 如果x是稠密矩阵, 会直接用它的空间
    def lu(x: BM[Double]): (BDM[Double], Array[Int]) = {
      val m = x.rows
      val n = x.cols
      val Y = x match {
        case m: BDM[Double] => m
        case _ => x.asInstanceOf[CSCMatrix[Double]].toDense
      }
      val ipiv = Array.ofDim[Int](scala.math.min(m, n))
      val info = new intW(0)
      lapack.dgetrf(m, n, Y.data, scala.math.max(1, m), ipiv, info)
      if (info.`val` < 0) throw new Exception("LU 分解失败")
      (Y, ipiv)
    }

    // 通过 LU分解得到的LU和P, 解方程
    def solve(lu: BDM[Double], p: Array[Int], b: BDV[Double]): BDV[Double] = {
      val n = b.length
      val x = b.copy
      val info = new intW(0)
      lapack.dgetrs("N", n, 1, lu.data, scala.math.max(1, n), p, x.data, scala.math.max(1, n), info)
      if (info.`val` < 0) throw new Exception("解方程失败")
      x
    }

    // Cholesky 分解, X = LLt
    // 这个的问题是不能使用稀疏方式来存输入的矩阵数据, 但我们的数据其实是非常稀疏的
    // 仔细研究算法以后发现稀疏方式意义不大, 因为在计算中途用到的数据都是之前迭代的结果, 这时的数据已经不再稀疏了
    def cholesky(x: BM[Double]): BDM[Double] = {
      // 使用BLT会导致方程求解时慢很多
      // val L = BLT(x)
      val L = x match {
        case m: BDM[Double] => m
        case _ => x.asInstanceOf[CSCMatrix[Double]].toDense
      }
      val n = x.rows
      val info = new intW(0)
      lapack.dpotrf("L", n, L.data, scala.math.max(1, n), info)
      if (info.`val` != 0) throw new Exception("Cholesky 分解失败")
      // 可将L用稀疏矩阵来存, 因为结果是个三角矩阵
      // 意义不大, 稀疏矩阵也需要额外空间来存行列索引, 因为分解后的结果并不稀疏, 算起来占用的空间差不多
      L
    }

    // 通过 Cholesky 分解得到的L, 求解
    // 如果L能用稀疏矩阵来存的话, 这个过程也是可以优化的
    // 大概试了一下, 10000*10000 的矩阵中非零项有 35825779, 略低于 50000000 的正常值, 优化程度有限
    // 而且整个算法的复杂度并不在这里, 没有必要再继续优化
    def solve(L: BM[Double], b: BDV[Double]): BDV[Double] = {
      val x = b.copy
      val n = b.length
      // LY = B => Y
      for (k <- 0 until n) {
        for (i <- 0 until k) {
          x(k) -= x(i) * L(k, i)
        }
        x(k) /= L(k, k)
      }

      // LtX = Y => X
      for (k <- n - 1 to 0 by -1) {
        for (i <- k + 1 until n) {
          x(k) -= x(i) * L(i, k)
        }
        x(k) /= L(k, k)
      }
      x
    }
  }






}
