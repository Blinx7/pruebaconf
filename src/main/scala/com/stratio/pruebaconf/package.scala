package com.stratio

import org.apache.spark.SparkContext
import com.stratio.pruebaconf.ErrorsOrHealthyFlightsDsl._


package object pruebaconf {


  var sc = new SparkContext("local", "exampleApp")
  var accumNa = 0
  var accumInt = 0
  var accumString = 0



  def main(args: Array[String]) {
    val data2 = sc.textFile("/home/gbecares/Desarrollo/spark-1.2.1-bin-hadoop2.4/1987bis.csv").correctFlight.collect.foreach(println)
    val fuel = sc.textFile("/home/gbecares/Desarrollo/spark-1.2.1-bin-hadoop2.4/Fuel-1987.csv")


    //val header = data2.first()
    //val data= data2.filter(!_.contains(header))


    /***************************************************************************************************************************/
    /*********************************************** PRUEBAS *******************************************************************/

    //Número de errores por tipo
    //val tiposDeErrores= data.flatMap(a=> analizarListaError(a.split(","))).map(a =>(a,1)).reduceByKey(_+_).collect.foreach(println)
    //lista de lineas con error
    //val errores= data.map(a=> (a,analizarListaError(a.split(",")))).filter(a=> a._2.contains("ErrorTipo1")|| a._2.contains("ErrorTipo2")).map(a=> a._1).collect.foreach(println)

    //Fichero saneado
    //val saneado= data.map(a=> (a,analizarListaError(a.split(",")))).filter(a=> a._2.contains("ErrorTipo1")|| a._2.contains("ErrorTipo2")).map(a=> a._1).collect.foreach(println)

    //lista de lineas con error y Lista d|e suma de errores por tipo
    //val errores= data.map(a=> analizarListaError(a,a.split(","))).flatMap(a=> a).flatMap(a=>a).collect.foreach(println)
    /***************************************************************************************************************************/
    /***************************************************************************************************************************/

    //Lista de errores en formato (linea, errortipoX)
    //val csvErrors = wrongFlight(data).collect.foreach(println)


    //csv saneado


   //print("There are "+accumNa+" NA erros, "+accumInt+" Int errors and "+accumString+" String errors")
   //arriveTimeMean(csvHealthy,"SFO","SAN")
   //arriveTimeMean2(csvHealthy,"SFO","SAN")
   //arriveTimeMean3(csvHealthy,"SFO","SAN")


  /*  val csvHealthy = correctFlight(data).map(vuelo=>((vuelo.Origin,vuelo.dates.Month),vuelo.Distance.toInt)).reduceByKey(_+_)//.sortByKey().collect.foreach(println)

    val monthAirportDistance  =   csvHealthy.map(month=>(month._1._2,(month._1._1,month._2)))//.collect().foreach(println)
    val fuelAirport= fuel.map(_.split(",")).map(month=> (month(1),(month(2)))).collect().toMap//.foreach(println)
    val fuels = sc.broadcast(fuelAirport)

    monthAirportDistance.map(price => (price._2._2*fuels.value(price._1).toInt,(price._1, price._2._1 ))).sortByKey().collect.foreach(println)*/
  }
  //def meanDistance(RDD : RDD[Flight],f: (Flight) ⇒ T,valor : Int): RDD[(String, Double)]= {
  //   RDD.keyBy(f(_)).groupByKey.mapValues(iterable=>iterable.toList.reduce(_+_).toDouble/iterable.toList.size).collect
  //  }

  //  def mean[T](RDD : RDD[Flight],f: (Flight) ⇒ T,valor : Int): RDD[(String, Double)]= {
  //   RDD.keyBy(f(_)).groupByKey.mapValues(iterable=>iterable.toList.reduce(_+_).toDouble/iterable.toList.size).collect
  //  }
/*  def arriveTimeMean(rdd : RDD[Flight], Dest : String, Orig : String): Unit= {
    rdd.filter(vuelo=>vuelo.Origin.contains(Orig) && vuelo.Dest.contains(Dest)).map(vuelo => ("ArriveTime mean between"+" "+Orig+" and "+Dest,vuelo.times.ArrTime.toInt)).groupByKey.mapValues(iterable=>iterable.toList.reduce(_+_).toDouble/iterable.toList.size).collect.foreach(println)
  }

  def arriveTimeMean2(rdd : RDD[Flight], Dest : String, Orig : String): Unit= {
    var media = rdd.filter(vuelo=>vuelo.Origin.contains(Orig) && vuelo.Dest.contains(Dest)).map(vuelo=>vuelo.times.ArrTime.toInt).mean()
    println("ArriveTime mean between"+" "+Orig+" and "+Dest+" "+media)
  }

  def arriveTimeMean3(rdd : RDD[Flight], Dest : String, Orig : String): Unit= {
    var rdd2 = rdd.filter(vuelo=>vuelo.Origin.contains(Orig) && vuelo.Dest.contains(Dest)).map(vuelo => (vuelo.times.ArrTime.toInt))
    var cuenta = rdd2.count().toInt
    var media = rdd2.reduce(_+_)/cuenta
    println("ArriveTime mean between"+" "+Orig+" and "+Dest+" "+media)

  }

  def wrongFlight(RDD : RDD[String]): RDD[(String, String)]= {
    RDD.map(linea=> analizarLista(linea,linea.split(","))).filter(_.isLeft).flatMap(_.left.get)
  }

  def correctFlight(RDD : RDD[String]): RDD[Flight]= {
    RDD.map(linea=> analizarLista(linea,linea.split(","))).filter(_.isRight).map(_.right.get).map(lineCorrect => Flight.apply(lineCorrect.split(",")))
    //toParsedFlight(flights).filter(_.isRight).map(_.right.get).map(lineCorrect => toFLight(lineCorrect))

  }*/

  //  def toFLight(Line : String): Flight ={
  //    val LineElements = Line.split((","))
  //    val date= Date(LineElements(0) ,LineElements(1) ,LineElements(2) ,LineElements(3))
  //    val time= Time(LineElements(4) ,LineElements(5) ,LineElements(6) ,LineElements(7) ,LineElements(11) ,LineElements(12) ,LineElements(13) )
  //    val delay= Delay(LineElements(14) ,LineElements(15) ,LineElements(24),LineElements(25),LineElements(26),LineElements(27),LineElements(28))
  //    val vuelo= Flight(date, time, delay, LineElements(8), LineElements(9) , LineElements(10) , LineElements(16),  LineElements(17),  LineElements(18) , LineElements(19),  LineElements(20),  LineElements(21) , LineElements(22),  LineElements(23))
  //
  //    vuelo
  //  }

/*  def analizarLista(linea : String, camposLinea : Seq[String]): Either[Seq[(String,String)], String]= {

    val columnasInt = Seq(0,1,2,3,4,5,6,7,9,11,12,14,15,18,21,23)
    val columnasString = Seq(8,16,17)
    val columnasNa = Seq(10,13,19,20,22,24,25,26,27,28)

    val enteros=  columnasInt.flatMap(columna => tryInt(linea,camposLinea(columna)))
    val cadenas=  columnasString.flatMap(columna => tryString(linea,camposLinea(columna)))
    val na=  columnasNa.flatMap(columna =>tryNa(linea,camposLinea(columna)))

    val salida= enteros ++ cadenas ++ na

    if((enteros ++ cadenas ++ na ).nonEmpty)
      Left(salida)
    else
      Right(linea)
  }

  def tryNa (linea : String,cadena: String): Option[(String,String)]={

    if (cadena.compareTo( "NA")==0)
      None
    else{
      accumNa+=1
      Some(("ErrorTipo3",linea))}
  }

  def tryString (linea : String,cadena: String): Option[(String,String)]={

    if(cadena.compareTo("NA")==0) {
      accumString += 1
      Some(("ErrorTipo1", linea))
    }else
      None
  }

  def tryInt(linea : String, cadena: String): Option[(String,String)]={
    try{
      if(cadena.compareTo("NA")==0) {
        accumInt += 1
        Some(("ErrorTipo1", linea))
      }else {
        cadena.toInt
        None
      }
    } catch {
      case e: Exception => accumInt+=1
        Some(("ErrorTipo2",linea))


    }
  }*/

  ////YA NO SE USA ESTA FUNCION
  //  def analizarListaError(linea : String, listaString : Seq[String]): Option[Seq[(String, String)]]= {
  //
  //    val columnasInt = Seq(0,1,2,3,4,5,6,7,9,11,12,14,15,18,21,23)
  //
  //    val columnasString = Seq(8,16,17)
  //
  //    val columnasNa = Seq(10,13,19,20,22,24,25,26,27,28)
  //
  //    val columna01= Seq(21,23)
  //    val prueba=Seq(0,2)
  //
  //    val enteros=  columnasInt.flatMap(a => tryInt(linea,listaString(a))).filter(a=> !a._2.isEmpty)
  //    val cadenas=  columnasString.flatMap(a => tryString(linea,listaString(a))).filter(a=> !a._2.isEmpty)
  //    val na=  columnasNa.flatMap(a =>tryNa(linea,listaString(a))).filter(a=> !a._2.isEmpty)
  //    //val salida=(enteros ++ cadenas ++ na) mkString(";")
  //
  //    if(!(enteros ++ cadenas ++ na ).isEmpty)
  //      Some(enteros++cadenas++na)
  //    else
  //      None
  //  }

}