import json
from pymongo import MongoClient
from pymongo.errors import (
    ServerSelectionTimeoutError,
    ConnectionFailure,
    PyMongoError
)

from config.config import (
    MONGO_CONNECTION_STRING
)

def data_storage(df_clima):
    print("\n--- Iniciando Almacenamiento y Operaciones Avanzadas en MongoDB ---")

    if df_clima.empty:
        print("El DataFrame está vacío. No hay datos para insertar o manipular en MongoDB.")
    else:
        try:
            client = MongoClient(MONGO_CONNECTION_STRING, serverSelectionTimeoutMS=5000)


            client.admin.command('ping')
            print("Conexión exitosa a MongoDB.")

            db = client["clima"]
            coleccion = db["datos_meteorologicos"]

            datos_para_mongo = df_clima.to_dict("records")

            print(f"\nBorrando documentos existentes en la colección '{coleccion.name}' para una inserción fresca...")
            delete_result = coleccion.delete_many({})
            print(f"Documentos eliminados: {delete_result.deleted_count}")

            print(f"Insertando {len(datos_para_mongo)} documentos en la colección '{coleccion.name}'...")
            insert_result = coleccion.insert_many(datos_para_mongo)
            print(f"Número de documentos insertados: {len(insert_result.inserted_ids)}.")

            print("\n--- Verificando datos en MongoDB con consultas avanzadas ---")

            print("\nDocumentos con las 5 temperaturas más altas (ciudad y temperatura):")
            for doc in coleccion.find({}, {'ciudad': 1, 'temperatura': 1, '_id': 0}).sort('temperatura', -1).limit(5):
                print(doc)

            print("\nTodos los documentos de 'Medellin':")
            for doc in coleccion.find({'ciudad': 'Medellin'}):
                print(json.dumps(doc, indent=2, default=str))

            print("\nDocumentos con humedad superior al 75% (ciudad y humedad):")
            for doc in coleccion.find({'humedad': {'$gt': 75}}, {'ciudad': 1, 'humedad': 1, '_id': 0}):
                print(doc)

            print("\n--- Actualizando un documento en MongoDB ---")
            update_query = {'ciudad': 'Bogota'}
            new_values = {'$set': {'nota_especial': 'Condiciones monitoreadas'}}
            update_result = coleccion.update_one(update_query, new_values)
            print(f"Documentos actualizados: {update_result.modified_count}")
            print("Documento de 'Bogota' después de la actualización:")
            print(json.dumps(coleccion.find_one({'ciudad': 'Bogota'}), indent=2, default=str))

            client.close()
            print("\nConexión a MongoDB cerrada exitosamente.")

        except ServerSelectionTimeoutError as err:
            print(f"ERROR: No se pudo conectar a MongoDB. Tiempo de espera excedido. Detalles: {err}")
            print("Verifique la accesibilidad de su servidor MongoDB:")
            print("1. Si es local, asegúrese de que el servicio `mongod` esté en ejecución y accesible en la dirección y puerto especificados (usualmente `localhost:27017`).")
            print("2. Si usa MongoDB Atlas, revise que su `MONGO_CONNECTION_STRING` sea correcta (usuario, contraseña, URL del clúster) y que su dirección IP esté permitida en 'Network Access' en el panel de Atlas.")
            print("3. Compruebe posibles configuraciones de firewall que puedan estar bloqueando la conexión (el puerto 27017 o 27017-27019 para Atlas).")
        except ConnectionFailure as err:
            print(f"ERROR: Fallo en la conexión a MongoDB. Detalles: {err}")
        except PyMongoError as err:
            print(f"ERROR: Un error específico de PyMongo ocurrió: {err}")
        except Exception as e:
            print(f"ERROR: Ocurrió un error inesperado al conectar o almacenar datos en MongoDB: {e}")
