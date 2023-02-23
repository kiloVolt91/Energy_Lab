##  ANALISI DATI edistribuzione
## CREAZIONE DI CLUSTER DI DATI RAGGRUPPATI PER MENSILITà
import numpy as np
import os
import pandas as pd
from datetime import datetime
from init import init_path
import configparser
import matplotlib.pyplot as plt
from matplotlib import ticker

config = configparser.ConfigParser()
config.read(init_path)

origin_path = config['user']['origin_path_an_edis']
pod_totali = config['user']['pod_totali']
anno = input('Inserire anno in formato YYYY: ')

pod =[]
dizionario_pod = {}
for i in range (1,int(pod_totali)+1):
    dizionario_pod[config.get('POD', 'p'+str(i))] = config.get('utenza', 'u'+str(i))

os.chdir(origin_path)
for name in os.listdir(origin_path):
    
    if name[0:6] =='IT001E':
        pod.append(name[0:18])
print('POD disponibili: ', pod)
i_pod = int(input('Inserisci il numero di posizione del POD desiderato dalla lista: '))

selezione = pod[i_pod-1]

print('Hai selezionato il seguente POD: ', selezione, dizionario_pod[selezione])

if os.path.exists(selezione) == True:
    os.chdir(selezione)
    if os.path.exists('e-dis') == True:
        os.chdir('e-dis')
        if os.path.exists(anno) == True:
            os.chdir(anno)
            path = os.getcwd()
else:
    print('Percorso non Trovato')    

tipo_energia = int(input("Tipologia di energia: inserisci 1 per l'energia attiva, inserisci 2 per l'energia reattiva: \n"))
tipo_regime = int(input("Tipologia di regime di cessione: inserisci 1 per l'immissione di energia, 2 per il prelievo di energia: \n"))

if (tipo_energia >=1) & (tipo_energia<=2):
    if tipo_energia == 1:
        tipo_energia = 'attiva'
    if tipo_energia == 2:
        tipo_energia = 'reattiva'
else:
    print('tipo energia: è stato inserito un valore errato')

if (tipo_regime >=1) & (tipo_regime<=2):
    if tipo_regime == 1:
        tipo_regime = 'immessa'
    if tipo_regime == 2:
        tipo_regime = 'prelevata'
else:
    print('tipo regime: è stato inserito un valore errato')    

nome_file = 'riepilogo_orario_'+tipo_energia+'_'+tipo_regime

if os.path.exists(path+'/' + nome_file + '.csv') == True:     
    df_day = pd.read_csv(path + '/' + nome_file + '.csv', sep = ';', index_col=False, decimal = ",")
else:
    print ('File non trovato: ')

vec_m = []
for i in range(0, df_day.shape[0]):
    vec_m.append(df_day.iloc[i,0][3:5])
df_day['mese'] = vec_m

df_op = df_day.copy()


operazioni = ['Somma', 'Media', 'Massimi', 'STD']

def crea_indice (df, operazione):
    vec_s = np.linspace(1,12,12, dtype = int)
    vec_t =[]
    for i in range (0, len(vec_s)):
        vec_t.append(operazione)
    coppie = list(zip(vec_s, vec_t))
    index = pd.MultiIndex.from_tuples(coppie, names = ['mese','op'])
    df.set_index(index, drop=True,inplace =True)
    return (df)

df_somme = df_day.groupby(by='mese').sum()
df_media = df_day.groupby(by='mese').mean()
df_massimi = df_day.groupby(by='mese').max()
df_std = df_day.groupby(by='mese').std()

df_somme = crea_indice(df_somme, 'Somma')
df_media = crea_indice(df_media, 'Media')
df_massimi = crea_indice(df_massimi, 'Massimi')
df_std = crea_indice(df_std, 'STD')

df_analisi = pd.concat([df_somme, df_media, df_std, df_massimi]).reset_index().drop(columns=['Giorno \\ Ora'])

data_path = os.path.join(origin_path, 'analisi_dati')


df_analisi.to_csv(data_path+'/riepilogo_'+selezione+'_'+ str(anno) +'.csv', index=False, decimal =',', sep=';')

