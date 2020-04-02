*******************************************************************************************
* Autor : Adil Ziani El Ali
* Fecha : 02/04/2020
*******************************************************************************************

En este ejemplo hacemos uso de SparkListener, clase que implementa eventos sobre las 
tareas Spark. Permite monitorizar y disponer de métricas de las tareas que se ejecutan 
en el framework.

En este ejemplo solo nos interesamos por contar númer de registros leidos o escritos.
Así pues, creamos una variable:

val test = TaskMetrics(SparkSesion)

Y aplicamos el método runAndMeasure(typeTask:String, task)
Ejemplo

test.runAndMeasure("read",df.show())

Teniendo en salida número de registros leidos al momento de cargarlos en dataframe df.
El cómputo de registros lo hemos obtenido sobre-escribiendo el método
onTaskEnd() de SparkListener y por tanto es el valor real, evitando así falsos valores que
se puedan dar por medio de shared variables debido a fallos en executros durante la realización
de las tareas.

Nota: Este ejemplo ha sido adaptado del proyecto  https://github.com/LucaCanali/sparkMeasure

