# Informe respecto al trabajo practico de coordinacion.

## Introduccion

Correspondiente a los problemas a resolver, se establecieron los siguientes objetivos:
- Finalizar la implementacion vacia del middleware.
- Crear un sistema en el server para el manejo de multiples clientes.
- Crear un sistema de coordinacion para SUM, en el cual se permita sincronizar varias instancias del mismo
para manejar mensajes de clientes al mismo tiempo sin perdida de datos
- Hacer lo mismo (o similar) con Aggregtion.
- Adaptar el archivo Join para manejar mensajes de varias instancias de Aggregation.
- Manejar la senal SIGTERM para garantizaar el graceful shutdown de todas las instancias.
- Garantizar la escalabilidad de los componentes.

Se enlista como se resolvio cada situacion.

## Middleware:

Para la implementacion del middleware, se incluyeron primitivas identicas a la implementacion realizada
en el trabajo practico de MOM realizado por mi.

## Manejo multiples Clientes

Para el manejo de multiples clientes, el Message Protocol crea un ID unico para cada cliente.
Este ID es enviado como informacion a las instancias de SUM, Aggregation y Join para representar
un cliente.

Las implementaciones de Sum, Aggregation y Join se adaptaron para mantener el total para cada
fruta, para cada cliente, en vez de solamente el total para cada fruta.
Esta informacion es eliminada del diccionario una vez recibida una senal EOF para ese ID de cliente.

## Coordinacion SUM

La idea original que habria usado para resolver la coordinacion de SUM era crear un Sum Manager 
que se encargue de coordinar todas las instancias. De esta forma, se podria relevar la responsabilidad
de coordinacion a un elemento externo.

Sin embargo, no es posible modificar los dockerfiles o el arhivo de docker compose para 
implementar estos servicios, por lo que sera necesario realizar la coordinacion entre las instancias de Sum.

Se tienen en cuenta las siguientes limitaciones:
- Gateway solamente va a enviar un unico mensaje de EOF a la layer de SUM, por lo que
unicamente una sola instancia de SUM lo va a recibir.
- Que una instacia de SUM reciba EOF para un ID, significa que esa instancia
no va a recibir mas mensajes de datos para ese ID.
- Que una instancia reciba EOF para ese ID, no significa que el resto de instancias de SUM
puedan recibir mensajes de datos de ese ID. Es decir, "Basta para mi no es basta para todos"

Hay que garantizar que, al momento de que las instancias de SUM hagan flush de sus mensajes (envien
todo a la capa de aggregation), hay que estar seguro de que todos los mensajes correspondientes a
ese ID hayan sido procesados por alguna instancia de SUM.

Esto se resolvio en dos partes:
1. Message_handler ahora cuenta la cantidad de mensajes procesados para ese ID y envia la informacion
como parte del mensaje para el EOF
2. Las instancias de SUM se comunican entre si para contar cuantos mensajes fueron procesados para ese
ID y para comunicarse entre ellos que ya pueden flushear los mensajes de esa ID.

Para esta segunda parte, se habia planteado anteriormente un sistema broadcast para comunicar la cantidad
de mensajes procesados una vez una instancia recibiese un EOF. Sin embargo, esto requiere una excesiva cantidad
de mensajes, haciendo Sum dificil de escalar (O(N*N) mensajes solo para coordinar).

Se comprometio encontrar una solucion que reduciese la cantidad de mensajes enviados por cada instancia.

Para este caso, se implemento una aquitectura de anillo a modo de comunicacion.

Funciona en el siguiente flujo:
- Cada instancia tiene la informacion de su propio ID y del ID de la siguiente instancia (en caso
de ser la ultima instancia se tiene informacion de la primera). Todas las instancias usan un unico
exchange compartido por todas las instancias (SUM_CONTROL_EXCHANGE). Cada instancia de Sum consume
los mensajes con su ID como routing_key y publica los mensajes de control hacia la siguiente routing_key.
    - En otras palabras, hay un exchange y N routing keys, una para cada instancia de SUM.
- Este exchange es separado del canal de datos, funciona como un canal de control donde se transmiten mensajes
para la coordinacion entre instancias.
- Cuando una instancia de SUM recibe un EOF, lo recibe junto con el ID del cliente y la cantidad de mensajes
enviados por ese cliente.
- La instancia de SUM que recibe el EOF se denomina a si mismo lo que yo llamo el "Token Master" de
ese ID. Esta instancia de SUM se va a encargar de asegurarse de que todos los mensajes de ese cliente
fueron procesados por las instancias de SUM.
- Para verificar esto, el token master hace lo siguiente de forma resumida:
    - Crea un mensaje token y lo manda por el canal de control a la siguiente instancia de SUM. Este
    mensaje tiene el objetivo de contar la cantidad de mensajes que fueron procesados por las instancias
    de SUM, para evitar race conditions, SUM usa un lock.
    - Este mensaje circula todo el anillo, contando los mensajes que cada instancia de SUM procesaron.
    - Cuando ese mensaje vuelve al Token Master de ese cliente, se evalua si la cantidad de mensajes
    contada es igual a la esperada.
    - Si es asi, el Token Master circula una orden de flush al resto de instancias de SUM, ordenandoles
    que envien todos los datos relacionados a la capa de aggregation.
    - Si no es asi, se recircula el token nuevamente.
- Cuando el Token Master circula una orden de flush, la instancia de SUM que la recibe:
    - Envia los datos relacionados al ID del cliente a la capa de aggregation, solo envia los datos
    a una instancia de aggregation.
    - borra los datos relacionados a ese ID.
    - circula la orden de Flush a la siguiente instancia de SUM.
- Cuando el Token Master vuelve a recibir la orden de flush que el envio, hace dos cosas
    - Flushea, al igual que el resto
    - Broacastea a todos los aggregators un EOF, que les indica que todos los datos de ese ID ya fueron enviados.

Esto minimiza la cantidad de mensajes a O(2N) por cada cliente, N mensajes de token + N ordenes de flush. Sin contar los casos donde hay que recastear el token. Esto es muy escalable ya que reduce la cantidad de mensajes redundantes al minimo.

## Coordinacion aggregation

Debido a que las instancias de SUM hacen el trabajo duro respecto a la coordinacion, las instancias de
Aggregator no coordinan entre si.
Debido a como reciben los datos:
- Si reciben un EOF para un ID, todas las instancias estan seguras de que ese EOF es el ultimo dato que van a recibir
de ese cliente. Esto es ya que cada instancia de SUM hace flush de sus datos antes de continuar la circulacion de
la orden de flush, y como solamente el Token Master broadcastea el EOF una vez todos hayan flusheado, se garantiza
que las instancias de Aggregation no van a recibir mas datos de ese ID una vez llega el EOF, por lo que cada instancia trabaja de forma independiente, calculando el top parcial y dejando que JOIN junte los elementos.

## Adaptacion de JOIN

Join recibe los tops parciales de cada instancia de Aggregation. Una vez recibe tops parciales de cada instancia,
calcula el top final y lo envia devuelta al cliente.
