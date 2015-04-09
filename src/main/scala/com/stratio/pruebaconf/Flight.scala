package com.stratio



case class Flight (dates: Date, times: Time, delays: Delay, UniqueCarrier: String, FlightNum: String, TailNum: String,	Origin: String,	Dest: String,	Distance: String,	TaxiIn: String,	TaxiOut: String,	Cancelled: String,	CancellationCode: String,	Diverted: String)
{


}

object Flight{
  def apply(fields: Array[String]): Flight = {
    val (firstChuck, secondChuck) = fields.splitAt(22)

    val Array(year, month,	dayofMonth,	dayOfWeek, departure, crsDeparture, arrive, crsArrive,
    uniqueCarrier, number, tailNumber, actualElapsed, crsElapsed, airTime, arriveDelay,
    departureDelay, origin, destination, distance, taxiIn, taxiOut, cancelled) = firstChuck

    val Array(cacellationCode, diverted, carrierDelay, weatherDelay, nasDelay, securityDelay,
    lateAircraftDelay) = secondChuck
    val date= Date(year, month,	dayofMonth,	dayOfWeek)
    val time= Time(departure ,crsDeparture ,arrive ,crsArrive ,actualElapsed ,crsElapsed ,airTime )
    val delay= Delay(arriveDelay ,departureDelay ,carrierDelay, weatherDelay, nasDelay, securityDelay, lateAircraftDelay)
    Flight(date, time, delay, uniqueCarrier, number, tailNumber, origin, destination, distance, taxiIn, taxiOut, cancelled , cacellationCode, diverted)
  }

  def analizarLista(linea : String, camposLinea : Seq[String]): Either[Seq[(String,String)], String]= {

    val columnasInt = Seq(0,1,2,3,4,5,6,7,9,11,12,14,15,18,21,23)
    val columnasString = Seq(8,16,17)
    val columnasNa = Seq(10,13,19,20,22,24,25,26,27,28)

    val enteros=  columnasInt.flatMap(columna => tryInt(linea,camposLinea(columna)))
    val cadenas=  columnasString.flatMap(columna => tryString(linea,camposLinea(columna)))
    val na=  columnasNa.flatMap(columna =>tryNa(linea,camposLinea(columna)))

    val salida=(enteros ++ cadenas ++ na)

    if(!(enteros ++ cadenas ++ na ).isEmpty)
      Left(salida)
    else
      Right(linea)
  }

  def tryNa (linea : String,cadena: String): Option[(String,String)]={

    if (cadena.compareTo( "NA")==0)
      None
    else
      Some(("ErrorTipo3",linea))
  }

  def tryString (linea : String,cadena: String): Option[(String,String)]={

    if(cadena.compareTo("NA")==0)
    {
      // errors.add("ErrorTipo1")
      Some(("ErrorTipo1",linea))
    }
    else
      None
  }

  def tryInt(linea : String, cadena: String): Option[(String,String)]={
    try{
      if(cadena.compareTo("NA")==0)
      { //errors.add("ErrorTipo1")
        Some(("ErrorTipo1",linea))
      }
      else
      {
        cadena.toInt
        None
      }
    } catch {
      case e: Exception =>
        //errors.add("ErrorTipo2")
        Some(("ErrorTipo2",linea))
    }
  }

}





case class Date(Year: String, Month: String,	DayofMonth: String,	DayOfWeek: String)
{}

case class Time(DepTime: String,	CRSDepTime: String, ArrTime: String,	CRSArrTime: String, ActualElapsedTime: String,	CRSElapsedTime: String,	AirTime: String)
{}

case class Delay(ArrDelay: String,	DepDelay: String,CarrierDelay: String,	WeatherDelay: String,	NASDelay: String,	SecurityDelay: String,	LateAircraftDelay: String)
{}