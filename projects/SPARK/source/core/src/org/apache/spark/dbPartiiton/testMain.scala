package org.apache.spark.sql.dbPartiiton

import com.zzjz.deepinsight.basic.BaseMain
import  org.apache.spark.sql.dbPartiiton.partition.Analysis._

object testMain extends  BaseMain{

  override  def  run():Unit={

    val  sql="SelEct  id,Sid,cii,bai,Xxx  from   tablename   WhEre   id>3  And  id<4   and   left<'8'  and  left>6 and  cxc>'4'  and  cvc<'100' "

    val  t=sqlExract(sql,"left")

    println(t)



  }

}
