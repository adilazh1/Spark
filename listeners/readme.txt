*******************************************************************************************
* Autor : Adil Ziani El Ali
* Fecha : 02/04/2020
*******************************************************************************************

En este ejemplo hacemos uso de SparkListener, clase que implementa eventos sobre las 
tareas Spark. Permite monitorizar y disponer de m�tricas de las tareas que se ejecutan 
en el framework.

En este ejemplo solo nos interesamos por contar n�mer de registros leidos o escritos.
As� pues, creamos una variable:

val test = TaskMetrics(SparkSesion)

Y aplicamos el m�todo runAndMeasure(typeTask:String, task)
Ejemplo

test.runAndMeasure("read",df.show())

Teniendo en salida n�mero de registros leidos al momento de cargarlos en dataframe df.
El c�mputo de registros lo hemos obtenido sobre-escribiendo el m�todo
onTaskEnd() de SparkListener y por tanto es el valor real, evitando as� falsos valores que
se puedan dar por medio de shared variables debido a fallos en executros durante la realizaci�n
de las tareas.

Nota: Este ejemplo ha sido adaptado del proyecto  https://github.com/LucaCanali/sparkMeasure

