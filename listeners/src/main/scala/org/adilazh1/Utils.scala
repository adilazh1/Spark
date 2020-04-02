package org.adilazh1

object Utils {

  def reformat(s:String,n:Long):String ={
    val name = s.toLowerCase()
    val basicValue = n.toString
    val optionalValueWithUnits = {
      if (name.contains("time") || name.contains("duration")) {
        " (" + 1.0*n/10000 + " s)"
      }
      else if (name.contains("bytes") || name.contains("size")) {
        " (" + 1.0*n/1024 + " Kb)"
      }
      else ""
    }
    s + " => " + basicValue + optionalValueWithUnits
  }
  }

