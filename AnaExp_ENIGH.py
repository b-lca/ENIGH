#Librerias
import numpy as np
import pandas as pd
import json
import os

### Limpieza de datos
pd.set_option('display.max_columns',None)
#Cargamos lso datos
df = pd.read_csv('gastoshogar.txt'
                 ,dtype={x:'str' for x in ['clave', 'forma_pag1', 'forma_pag2', 'forma_pag3', 'frecuencia', 'inst_1', 'inst_2', 'lugar_comp', 'orga_inst', 'tipo_gasto', 'mes_dia', 'fecha_adqu', 'fecha_pago', 'folioviv', 'foliohog']}
                 ,na_values=['0000','00','0']
                 ,quotechar="'"
                 )
df.head()
df.info()

## 1.1) Valores numéricos. Crea una función que reciba un DataFrame y una lista de
#columnas que deban ser convertidas de tipo de dato str a float, la función debe
#devolver el dataframe con todas sus columnas, cambiando el tipo de dato de las
#seleccionadas
def valores_numericos(data:pd.DataFrame, columnas:list):
    """Recibe un DataFrame y un listado de columnas y transforma el tipo de dato de estas ultimas a flotante"""
    data_out = data.copy() #recuerda hacer un copy de la tabla a manipular en la función para evitar problemas de relación
    for columna in columnas:
        data_out[columna] = pd.to_numeric(data_out[columna],errors='coerce').astype(float)
    return data_out

## 1.2) Columnas nulas. Crea una función que reciba un DataFrame y un cutoff para
#determinar el porcentaje mínimo de valores no nulos en la tabla para cada columna,
#elimina las columnas que tengan más de dicho porcentaje nulo
#(_% de valores no nulos_ > min_pct_no_nulo)
def no_nulo(data:pd.DataFrame, min_pct_no_nulo:float=0.60):
    data_out = data.copy() #recuerda hacer un copy de la tabla a manipular en la función para evitar problemas de relación
    for columna in data.columns:
        if  data[columna].notnull().sum()/data_out.shape[0] < min_pct_no_nulo:
            data_out.drop(columna, axis = 1, inplace = True)
    return data_out

## 1.3) Columnas unarias. Crea una función que reciba un DataFrame, una lista de
#columnas y un cutoff. el uso de esta función determinara que columnas de variables
#discretas tienen un "único" valor, si el valor más frecuente se repite más del
#porcentaje determinado en el cutoff, la columna se remueve del DataFrame resultante
def no_unarias(data: pd.DataFrame, columnas: list, max_pct_repeticion: float = 0.90):
    data_out = data.copy()
    for columna in columnas:
        if columna in data_out.columns:
            value_counts = data_out[columna].value_counts()
            if value_counts.max() / data_out.shape[0] > max_pct_repeticion:
                data_out.drop(columna, axis=1, inplace = True)
    return data_out

#1.4) Limpieza de datos. Crea una función que le aplique las funciones de limpieza
#al DataFrame en el orden de: valores_numericos, no_nulo, no_unarias, por lo que los
#parametros son las usadas en la funciones previas
def limpieza_de_datos(data:pd.DataFrame, columnas_numericas:list, columnas_no_numericas:list, min_pct_no_nulo:float=0.60, max_pct_repeticion:float=0.90):
    data_out = data.copy() #recuerda hacer un copy de la tabla a manipular en la función para evitar problemas de relación
    data_out = valores_numericos(data_out, columnas_numericas)
    data_out = no_nulo(data_out,min_pct_no_nulo)
    data_out = no_unarias(data_out, columnas_no_numericas, max_pct_repeticion)
    return data_out

columnas_numericas = ['cantidad', 'gasto', 'pago_mp', 'costo', 'inmujer', 'num_meses', 'num_pagos', 'gasto_tri', 'gasto_nm', 'gas_nm_tri', 'imujer_tri']
columnas_no_numericas = ['clave', 'tipo_gasto', 'mes_dia', 'forma_pag1','forma_pag2', 'forma_pag3', 'lugar_comp', 'orga_inst', 'frecuencia','fecha_adqu', 'fecha_pago', 'inst_1', 'inst_2', 'ultim_pago']

df = limpieza_de_datos(
    data=df
    ,columnas_numericas=columnas_numericas
    ,columnas_no_numericas=columnas_no_numericas
    ,min_pct_no_nulo=0.7
    ,max_pct_repeticion=0.9
    )
df.head()

columnas_numericas = [x for x in columnas_numericas if x in df.columns]
columnas_no_numericas = [x for x in columnas_no_numericas if x in df.columns]

### Cruce de tablas
catalogos = os.listdir('./catalogos/')

cat = dict()
for x in catalogos:
    read = open(f'./catalogos/{x}','r')
    json_r = json.loads(read.read())
    read.close()
    json_r = pd.DataFrame(json_r['var_catgry'])
    name = x.split('.')[0]
    json_r.rename(columns={'value':name,'labl':f'{name} codificación'},inplace=True)
    cat[name] = json_r[[name,f'{name} codificación']]

# 2.1) Genera un bucle que, para todas las `columnas_no_numericas` de los cuales se
#cuenta con un catálogo en el diccionario `cat` se agregue su columna con el valor
#codificado de las columnas
for col in columnas_no_numericas:
    try:
        df = df.merge(cat[col],how='left')
    except:
        pass
df.info()

#------------------------------------------------------------------------------
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
datos = df['gasto'][df['gasto'].notnull()]
#Creamos categorias de los gastos
len(datos)
#Contamos con 1280294 datos sobre el gasto
len(datos[datos <= 100])
#Donde 945418 son gastos que no superan los 100 pesos
len(datos[datos <= 100])/len(datos)
#Es decir, el 73.84% de los gastos disponibles no superan los 100 pesos
len(datos[(datos > 100) & (datos <= 1000)])
#Contamos con 287664 gastos que superan los 100 pesos pero son menores a 1000 pesos
len(datos[(datos > 100) & (datos <= 1000)])/len(datos)
#Es decir el 22.46% de los gastos disponibles no superan los 1000 pero si superan los 100
#Hasta este punto, se tiene que el 96.31% de los datos sobre los gastos disponibles, no superan los 1000 pesos
len(datos[(datos > 1000) & (datos <= 10000)])
#Tenemos que 43809 gastos se encuentran el el rango [1001,10000]
len(datos[(datos > 1000) & (datos <= 10000)])/len(datos)
#Es decir el 3.42% de dichos datos se encuentran en dicho rango
#Hasta el momento se tiene que el 99.73% de los gastos son menores a 10000 pesos
len(datos[(datos > 10000) & (datos <= 100000)])
#Tenemos que solo 3266 gastos se encuentran en el rango de [10001,100000]
len(datos[(datos > 10000) & (datos <= 100000)])/len(datos)
#Es decir el 0.255% de los gastos se encuentran en dicho rango
len(datos[(datos > 100000) & (datos < 1000000)])
#En este último rango, tenemos que solo 137 gastos
len(datos[(datos > 100000) & (datos < 1000000)])/len(datos)
#Es decir, solo el 0.0107% de los gastos se encuentra en este rango

plt.hist(df['gasto'][df['gasto'].notnull()], bins = 10, range = (0.5,100))
# Agregar etiquetas y título
plt.xlabel('Valores')
plt.ylabel('Frecuencia')
plt.title('Gastos menores o iguales a 100')

# Mostrar el histograma
plt.show()

plt.hist(df['gasto'][df['gasto'].notnull()], bins = 10, range = (101,1000))
# Agregar etiquetas y título
plt.xlabel('Valores')
plt.ylabel('Frecuencia')
plt.title('Gastos mayores que 100 pero menores o iguales a 1000')

# Mostrar el histograma
plt.show()

plt.hist(df['gasto'][df['gasto'].notnull()], bins = 10, range = (1001,10000))
# Agregar etiquetas y título
plt.xlabel('Valores')
plt.ylabel('Frecuencia')
plt.title('Gastos mayores que 1000 pero menores o iguales a 10000')

# Mostrar el histograma
plt.show()

plt.hist(df['gasto'][df['gasto'].notnull()], bins = 10, range = (10001,100000))
# Agregar etiquetas y título
plt.xlabel('Valores')
plt.ylabel('Frecuencia')
plt.title('Gastos mayores que 10000 pero menores o iguales a 1000000')

# Mostrar el histograma
plt.show()

plt.hist(df['gasto'][df['gasto'].notnull()], bins = 10, range = (100001,1000000))
# Agregar etiquetas y título
plt.xlabel('Valores')
plt.ylabel('Frecuencia')
plt.title('Gastos mayores que 100000 pero menores o iguales a 1000000')

# Mostrar el histograma
plt.show()

def asignar_rural_urbano(folio):
    if folio[2] == '6':
        return 'rural'
    else:
        return 'urbano'
df['rural_urbano'] = df['folioviv'].apply(asignar_rural_urbano)
print(df)

df_rural = df[df['rural_urbano'] == 'rural']
df_urbano = df[df['rural_urbano'] == 'urbano']
datos_rural = df_rural['gasto'][df_rural['gasto'].notnull()]
datos_urbano = df_urbano['gasto'][df_urbano['gasto'].notnull()]

len(datos_rural)
#Contamos con 458012 registros de areas rurales
len(datos_urbano)
#Contamos con 822282 registros de areas urbanas
len(datos_rural[datos_rural <= 100])
len(datos_urbano[datos_urbano <= 100])
#Tenemos que 351286 registros de gastos de areas rurales no superan los 100 pesos
#Mientras que 594132 registros de gastos de areas urbanas no  superan los 100 pesos
len(datos_rural[datos_rural <= 100])/len(datos_rural)
len(datos_urbano[datos_urbano <= 100])/len(datos_urbano)
#Es decir, el 76.69% de los gastos disponibles para areas rurales no superan los 100 pesos
#Mientras que en areas urbanas el 72.25% de los gastos no superan los 100 pesos
len(datos_rural[(datos_rural > 100) & (datos_rural <= 1000)])
len(datos_urbano[(datos_urbano > 100) & (datos_urbano <= 1000)])
#Tenemos que 94076 registros de gastos de areas rurales no superan los 1000 pesos, pero si superan los 100 pesos
#Mientras que 193588 registros de gastos de areas urbanas no  superan los 1000 pesos, pero si superan los 100
len(datos_rural[(datos_rural > 100) & (datos_rural <= 1000)])/len(datos_rural)
len(datos_urbano[(datos_urbano > 100) & (datos_urbano <= 1000)])/len(datos_urbano)
#Es decir el 20.54% de los gastos disponibles para areas rurales no superan los 1000 pero si superan los 100
#Mientras que el 23.54% de los gastos disponibles para areas urbanas no superan los 1000 pero si superan los 100
#Hasta este punto, se tiene que el 97.23% de los datos sobre los gastos disponibles para areas rurales, no superan los 1000 pesos
#Y para las areas urbanas se tiene que el 95.79 de los gastos no superan los 1000
len(datos_rural[(datos_rural > 1000) & (datos_rural <= 10000)])
len(datos_urbano[(datos_urbano > 1000) & (datos_urbano <= 10000)])
#Tenemos que 11653 gastos de areas rurales se encuentran en el rango [1001,10000]
#Y 32156 gastos de areas urbanas se encuentran en el rango [1001,10000]
len(datos_rural[(datos_rural > 1000) & (datos_rural <= 10000)])/len(datos_rural)
len(datos_urbano[(datos_urbano > 1000) & (datos_urbano <= 10000)])/len(datos_urbano)
#Es decir el 2.54% de los gastos disponibles para areas rurales no superan los 10000 pero si superan los 1000
#Mientras que el 3.91% de los gastos disponibles para areas urbanas no superan los 10000 pero si superan los 1000
#Hasta este punto, se tiene que el 99.77% de los datos sobre los gastos disponibles para areas rurales, no superan los 10000 pesos
#Y para las areas urbanas se tiene que el 99.7 de los gastos no superan los 100,000
len(datos_rural[(datos_rural > 10000) & (datos_rural <= 100000)])
len(datos_urbano[(datos_urbano > 10000) & (datos_urbano <= 100000)])
#Tenemos que 967 gastos de areas rurales se encuentran en el rango [10001,100000]
#Y 2299 gastos de areas urbanas se encuentran en el rango [10001,100000]
len(datos_rural[(datos_rural > 10000) & (datos_rural <= 100000)])/len(datos_rural)
len(datos_urbano[(datos_urbano > 10000) & (datos_urbano <= 100000)])/len(datos_urbano)
#Es decir el 0.2111% de los gastos disponibles para areas rurales no superan los 100000 pero si superan los 10000
#Mientras que el 0.2795% de los gastos disponibles para areas urbanas no superan los 100000 pero si superan los 10000
len(datos_rural[(datos_rural > 100000) & (datos_rural < 1000000)])
len(datos_urbano[(datos_urbano > 100000) & (datos_urbano < 1000000)])
#Tenemos que 30 gastos de areas rurales se encuentran en el rango [10001,100000]
#Y 107 gastos de areas urbanas se encuentran en el rango [10001,100000]
len(datos_rural[(datos_rural > 100000) & (datos_rural < 1000000)])/len(datos_rural)
len(datos_urbano[(datos_urbano > 100000) & (datos_urbano < 1000000)])/len(datos_urbano)
#Es decir el 0.00655% de los gastos disponibles para areas rurales no superan los 1000000 pero si superan los 100000
#Mientras que el 0.01301% de los gastos disponibles para areas urbanas no superan los 1000000 pero si superan los 100000

#Graficos rural
plt.hist(datos_rural, bins = 10, range = (0.5,100))
# Agregar etiquetas y título
plt.xlabel('Valores')
plt.ylabel('Frecuencia')
plt.title('Gastos menores o iguales a 100')

# Mostrar el histograma
plt.show()

plt.hist(datos_rural, bins = 10, range = (101,1000))
# Agregar etiquetas y título
plt.xlabel('Valores')
plt.ylabel('Frecuencia')
plt.title('Gastos mayores que 100 pero menores o iguales a 1000')

# Mostrar el histograma
plt.show()

plt.hist(datos_rural, bins = 10, range = (1001,10000))
# Agregar etiquetas y título
plt.xlabel('Valores')
plt.ylabel('Frecuencia')
plt.title('Gastos mayores que 1000 pero menores o iguales a 10000')

# Mostrar el histograma
plt.show()

plt.hist(datos_rural, bins = 10, range = (10001,100000))
# Agregar etiquetas y título
plt.xlabel('Valores')
plt.ylabel('Frecuencia')
plt.title('Gastos mayores que 10000 pero menores o iguales a 1000000')

# Mostrar el histograma
plt.show()

plt.hist(datos_rural, bins = 10, range = (100001,1000000))
# Agregar etiquetas y título
plt.xlabel('Valores')
plt.ylabel('Frecuencia')
plt.title('Gastos mayores que 100000 pero menores o iguales a 1000000')

# Mostrar el histograma
plt.show()

#Graficos urbano
plt.hist(datos_urbano, bins = 10, range = (0.5,100))
# Agregar etiquetas y título
plt.xlabel('Valores')
plt.ylabel('Frecuencia')
plt.title('Gastos menores o iguales a 100')

# Mostrar el histograma
plt.show()

plt.hist(datos_urbano, bins = 10, range = (101,1000))
# Agregar etiquetas y título
plt.xlabel('Valores')
plt.ylabel('Frecuencia')
plt.title('Gastos mayores que 100 pero menores o iguales a 1000')

# Mostrar el histograma
plt.show()

plt.hist(datos_urbano, bins = 10, range = (1001,10000))
# Agregar etiquetas y título
plt.xlabel('Valores')
plt.ylabel('Frecuencia')
plt.title('Gastos mayores que 1000 pero menores o iguales a 10000')

# Mostrar el histograma
plt.show()

plt.hist(datos_urbano, bins = 10, range = (10001,100000))
# Agregar etiquetas y título
plt.xlabel('Valores')
plt.ylabel('Frecuencia')
plt.title('Gastos mayores que 10000 pero menores o iguales a 1000000')

# Mostrar el histograma
plt.show()

plt.hist(datos_urbano, bins = 10, range = (100001,1000000))
# Agregar etiquetas y título
plt.xlabel('Valores')
plt.ylabel('Frecuencia')
plt.title('Gastos mayores que 100000 pero menores o iguales a 1000000')

# Mostrar el histograma
plt.show()
