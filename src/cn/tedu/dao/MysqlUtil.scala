package cn.tedu.dao
import java.sql.{Connection, Date, PreparedStatement, ResultSet}
import java.text.SimpleDateFormat

import cn.tedu.pojo.{Tongji, tongjiBean}
import com.mchange.v2.c3p0.ComboPooledDataSource
object MysqlUtil {
  //获取c3p0连接池对象
  val dataSource=new ComboPooledDataSource()

  def save(tongjiBean: Tongji) = {

    var conn:Connection=null
    var ps1:PreparedStatement=null
    var rs:ResultSet=null
    var ps2:PreparedStatement=null
    var ps3:PreparedStatement=null

    try{
      conn=dataSource.getConnection
      val sdf=new SimpleDateFormat("yyyy-MM-dd")
      val todayTime=sdf.format(tongjiBean.time)
      //先查询tongji2表当天是否有数据,如果没有数据则插入,如果有数据则更新
      ps1=conn.prepareStatement("select * from tongji2 where reporttime=?")

      ps1.setString(1,todayTime)

      rs=ps1.executeQuery()

      if(rs.next()){
        //当天已经有数据,则执行更新
        ps2=conn.prepareStatement("update tongji2 set pv=pv+?,uv=uv+?,vv=vv+?,newip=newip+?,newcust=newcust+? where reporttime=?")
        ps2.setInt(1,tongjiBean.pv)
        ps2.setInt(2,tongjiBean.uv)
        ps2.setInt(3,tongjiBean.vv)
        ps2.setInt(4,tongjiBean.newIp)
        ps2.setInt(5,tongjiBean.newCust)
        ps2.setString(6,todayTime)

        ps2.executeUpdate()

      }else{
        //当天还没有数据,则新增
        ps3=conn.prepareStatement("insert into tongji2 values(?,?,?,?,?,?)")
        ps3.setString(1,todayTime)
        ps3.setInt(2,tongjiBean.pv)
        ps3.setInt(3,tongjiBean.uv)
        ps3.setInt(4,tongjiBean.vv)
        ps3.setInt(5,tongjiBean.newIp)
        ps3.setInt(6,tongjiBean.newCust)

        ps3.executeUpdate()
      }


    }catch{
      case t: Throwable => throw t
    }finally{
      if(ps3!=null)ps3.close()
      if(ps2!=null)ps2.close()
      if(rs!=null)rs.close()
      if(ps1!=null) ps1.close()
      if(conn!=null) conn.close()
    }


  }

}
