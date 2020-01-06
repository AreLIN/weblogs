package cn.tedu.pojo

/**
 * case 样例类,和普通的scala class的区别
 * 1.必须显式的声明一个主构造器
 * 2.cose clase会自动构造一个空的构造器
 * 3.case clase会自动混入序列化特质
 * 4.会自动实现tostring
 * 5.创建对象不用new,就可以创建类的实例对象
 */
case class LogBean(val url:String,val urlname:String,val uvid:String,val ssid:String,
                   val sscount:String,val sstime:String,val cip:String)
