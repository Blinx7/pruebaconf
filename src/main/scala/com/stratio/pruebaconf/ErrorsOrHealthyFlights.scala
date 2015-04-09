package com.stratio.pruebaconf

import com.stratio.Flight
import org.apache.spark.rdd.RDD
/**
 * Created by pmadrigal on 7/04/15.
 */
class ErrorsOrHealthyFlights(rdd: RDD[String]) extends Serializable{



  def toParsedFlight: RDD[Either[Seq[(String,String)], String]] ={
    val header=rdd.first()
    rdd.filter(!_.contains(header)).map(linea=> Flight.analizarLista(linea,linea.split(",")))
  }

  def wrongFlight: RDD[(String, String)]= {
    toParsedFlight.filter(_.isLeft).flatMap(_.left.get)
  }

  def correctFlight: RDD[Flight]= {
    toParsedFlight.filter(_.isRight).map(_.right.get).map(lineCorrect => Flight.apply(lineCorrect.split(",")))
  }


}


trait ErrorsOrHealthyFlightsDsl {

  implicit def ErrorsOrHealthyFlights(rdd: RDD[String]): ErrorsOrHealthyFlights = new ErrorsOrHealthyFlights(rdd)
}

object ErrorsOrHealthyFlightsDsl extends ErrorsOrHealthyFlightsDsl