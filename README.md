# SD-Lab2-Streaming-aws

## Integrantes:
* Luis Aguilar
* Juan De Pablo
* Gabriel Venegas

## Descripción del problema
  El problema planteado es: servicio streaming que sea capaz de leer un dato de otro emisor streaming, realizar una evaluación en algun motor de reglas o un cruce con base de datos en memoría y posterior emisión de dato a la red bajo patron publish-suscribe.
  
  Como requisito funcional: Lenguaje de programación libre dentro del stack de spark.
  
  Como proveedor cloud: Amazon Web Services
  
## Enfoque de la solución
La solución desarrollada se realiza mediante "Kinesis" de AWS, el cual permite el trato de stream de datos y que permite utilizar patrón plubish-suscriber.
El enfoque de la solución es una combinación de patrones entre productor-consumidor y publicación-suscriptor. La parte de productor es utilizada para generar un stream de datos, a continuación se presenta un diagrama de como funciona Kinesis.


### Kinesis
![kinesis3](https://user-images.githubusercontent.com/19898908/58143990-bfc71500-7c1a-11e9-9ed1-96deb24cc20f.PNG)

## Desarrollo

### Principales inconvenientes o barreras detectadas
Los principales incovenientes fueron la poca experiencia en la utilización de AWS y spark, lo cual llevo un tiempo de aprendizaje de estas tecnologías antes de encontrar una solución al problema planteado. También existieron dificultades para comprender lo que se pedía para este laboratorio, dado a su vez, por el desconocimiento de las tecnologías que se debian utilizar.

Entre las barreras detectadas, esta que para utilizar AWS se requiere una tarjeta de crédito que cuente con compra internacional. Si no se cuenta con un sistema de pago habilitado AWS no deja probar sus servicios
### Clases/funciones/procedimientos principales del desarrollo

Datos para la creación del servicio de data stream

Se define 3 parámetros de la configuración del servicio de stream de datos. Estos son el nombre del stream, la región del servidor y un nombre para el AWS.


Definición de funciones para crear el stream.

  A continuación se definen las funciones que permiten la creación del servicio de stream de datos alojado en el AWS.
	get_status(): método que se encarga de verificar si esta activo o no el servicio.
  create_stream(): método que se encarga de la creación del servicio stream a través de la función create_stream de kinesis.


 Definición funciones creación del data stream.
 
  KinesisProducer(): Clase que permite la creación del productor de streams de datos en el AWS.
	put_record(): método que inicia la ejecución de los stream de datos.
	run_continously(): método que se encarga de la ejecución periódica de los stream de datos.
	run(): función que inicia la ejecución del productor.

 Definición funciones creación del data stream.
 
  KinesisConsumer(): Clase que permite la creación del consumidor de los stream de datos emitidos por el productor alojado     en el AWS.
  iter_records(): método que permite al consumidor iterar los datos emitidos por el productor desde un bloque definido.
  run(): método que se encarga de iniciar el consumidor, definir el inicio y término de este y en el caso de que se hayan       terminado los datos emitidos finaliza su ejecución.KinesisConsumer(): Clase que permite la creación del consumidor de los   	stream de datos emitidos por el productor alojado en el AWS.
  iter_records(): método que permite al consumidor iterar los datos emitidos por el productor desde un bloque definido.
  run(): método que se encarga de iniciar el consumidor, definir el inicio y término de este y en el caso de que se hayan       terminado los datos emitidos finaliza su ejecución.
  
  Redefinición de función de procesamiento de data para búsqueda por rut.
  
  SearchConsumer(): clase que se encarga de que el consumidor busque un dato emitido por el porductor.
   process_records(): método que se encarga de que el consumidor pueda encontrar el dato dentro del streaming de datos         recibidos desde el productor.
 
 Redefinición de función de procesamiento de data para búsqueda por rut.
 
  SearchConsumer(): clase que se encarga de que el consumidor busque un dato emitido por el porductor.
   process_records(): método que se encarga de que el consumidor pueda encontrar el dato dentro del streaming de datos         recibidos desde el productor.



## Resultados (gráficos de evaluación de tiempos de respuesta, capturas de pantalla de la plataforma funcional, entre otros).
![data stream](https://user-images.githubusercontent.com/19898908/58142929-f3a03b80-7c16-11e9-8608-7383b3266083.PNG)
	
Consumidor captando el streaming del Productor.
![data_stream](https://user-images.githubusercontent.com/28808828/58146570-b2168d00-7c24-11e9-98b4-afdb10676246.png)

## Link de acceso a versión productiva del software.

## Pasos para desplegar servicio desde cero.
