****************************************************************************
Fecha: 2020/03/06
Autor: adilazh1
****************************************************************************
En este ejercicio practicamos insertar datos en hdfs desde Spark,
creando "tablas" hive.

También practicamos como manipular documentos JSON para convertirlos en 
dataframes, con la librería BSON de MongoDB.

Las pruebas se realizan en el seguiente entorno:
  --vm cloudera quickstart 5.5.0 
  --scala version 2.12.10 
  --Spark version 2.2.4

El fichero de entrada /resources/bicing.json corresponde a un documento
descargado de open data Barcelona, que muestra información sobre las 
estaciones bicing, servicio de bici de la ciudad.
https://opendata-ajuntament.barcelona.cat/data/en/dataset