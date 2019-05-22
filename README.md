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
El enfoque de la solución es una combinación de patrones entre productor-consumidor y publicación-suscriptor. La parte de productor es utilizada para generar un stream de datos, a continuación 

## Desarrollo

### Principales inconvenientes o barreras detectadas
Los principales incovenientes fueron la poca experiencia en la utilización de AWS y spark, lo cual llevo un tiempo de aprendizaje de estas tecnologías antes de encontrar una solución al problema planteado. También existieron dificultades para comprender lo que se pedía para este laboratorio, dado a su vez, por el desconocimiento de las tecnologías que se debian utilizar.

Entre las barreras detectadas, esta que para utilizar AWS se requiere una tarjeta de crédito que cuente con compra internacional. Si no se cuenta con un sistema de pago habilitado AWS no deja probar sus servicios
### Clases/funciones/procedimientos principales del desarrollo

## Resultados (gráficos de evaluación de tiempos de respuesta, capturas de pantalla de la plataforma funcional, entre otros).
![data stream](https://user-images.githubusercontent.com/19898908/58142929-f3a03b80-7c16-11e9-8608-7383b3266083.PNG)
## Link de acceso a versión productiva del software.

## Pasos para desplegar servicio desde cero.
