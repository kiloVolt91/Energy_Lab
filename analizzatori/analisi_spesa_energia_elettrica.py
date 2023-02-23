#CREAZIONE DI UN FILE .CSV DI REPORT CONTENENTE I COSTI ASSOCIATI ALL'ACQUISTO O ALLA VENDITA DELL'ENERGIA ELETTRICA
## LO SCRIPT NECESSITA DEI DATI ESTRATTI DALLE CURVE E-DISTRIBUZIONE ED I DATI ESTRATTI DAL SITO GME COME DA SCRIPT PRECEDENTI

import os
import pandas as pd
from datetime import datetime
import math
import configparser

#posizione del file init.ini
#N.B. verificare la correttezza di tutti i percorsi
init_path = '/home/episciotta/Documenti/SVILUPPO/repo_sviluppo_ctz/dati_configurazione/configurazione.ini'

#apertura del file init
config = configparser.ConfigParser()
config.read(init_path)
origin_path = config['user']['origin_path_an_prezzi']
pod_totali = config['user']['pod_totali']

#creazione del dizionario contenente i pod e la relativa descrizione - vedi file init.ini
#N.B. inserire all'interno del dizionario tutti i POD disponibili e la relativa descrizione
dizionario_pod = {}
for i in range (1,int(pod_totali)+1):
    dizionario_pod[config.get('POD', 'p'+str(i))] = config.get('utenza', 'u'+str(i))
    
# SCANSIONE DEI POD PRESENTI ALL'INTERNO DELLA CARTELLA E SELEZIONE DI POD E ANNO
file_path = os.chdir(origin_path)
pod = []
for name in os.listdir(file_path):
    if name[0:6] =='IT001E':
        pod.append(name[0:18])
print('POD disponibili: ', pod)
i_pod = int(input('Inserisci il numero di posizione del POD desiderato dalla lista: '))
selezione = pod[i_pod-1]
print('Hai selezionato il seguente POD: ', selezione, dizionario_pod[selezione]) 
anno = input('Inserire anno in formato YYYY: ')
print('Hai selezionato il seguente anno: ', anno)

# SELEZIONE DELLA TIPOLGIA DI REGIME COMMERCIALE
tipo_regime = int(input("Tipologia di regime di cessione: inserisci 1 per l'immissione di energia, 2 per il prelievo di energia: \n"))
tipo_prezzo = int(input("Tipologia di prezzo praticato: 1 per l'acquisto di energia al PUN, inserisci 2 per la vendita di energia a PZ-SICI, inserisci 3 per la vendita di energia a PZ-NORD : \n"))
if (tipo_regime >=1) & (tipo_regime<=2):
    if tipo_regime == 1:
        tipo_regime = 'immessa'
    if tipo_regime == 2:
        tipo_regime = 'prelevata'
else:
    print('tipo regime: è stato inserito un valore errato')    
if (tipo_prezzo >=1) & (tipo_prezzo<=3):
    if tipo_prezzo == 1:
        tipo_prezzo = 'pun'
    if tipo_prezzo == 2:
        tipo_prezzo = 'pz_sici'
    if tipo_prezzo == 3:
        tipo_prezzo = 'pz_nord'
else:
    print('tipo prezzo: è stato inserito un valore errato') 
      
#INDIVIDUAZIONE ED ESTRAZIONE DEI DATI SORGENTE    
os.chdir(selezione)    
nome_file_energia = 'riepilogo_orario_attiva_'+tipo_regime #file contenente i consumi energetici
nome_file_prezzi = 'riepilogo_'+tipo_prezzo #file contenente i prezzi orari dei prodotti energetici
file_path_energia = os.path.join(origin_path, selezione, 'e-dis', str(anno))
if os.path.exists(file_path_energia) == True:      #+'/' + nome_file_energia + '.csv')
    df_energia = pd.read_csv(file_path_energia+'/'+nome_file_energia+'.csv', sep = ';', index_col=False, decimal = ",")
else:
    print ('File non trovato: ')
file_path_prezzi = os.path.join(origin_path, 'gme', str(anno))
if os.path.exists(file_path_prezzi+'/' + nome_file_prezzi + '.csv') == True:     
    df_prezzi = pd.read_csv(file_path_prezzi + '/' + nome_file_prezzi + '.csv', sep = ';', index_col=False, decimal = ",")
else:
    print ('File non trovato: ')
    
#MANIPOLAZIONE DEI DATI SORGENTE - ESTRAPOLAZIONE DELLE SPESE PER CIASCUNA ORA DI CIASCUN GIORNO E COMPLESSIVO MENSILE
df_spesa_oraria = df_prezzi.copy()
for row in range (0,df_spesa_oraria.shape[0]):
    for column in range (1,df_spesa_oraria.shape[1]):
        df_spesa_oraria.iloc[row,column] = df_energia.iloc[row,column]*df_prezzi.iloc[row,column]/1000 #prodotto energia*costo energia. La divisione per 1000 è necessaria per convertire i MWh in kWh
df_spesa_oraria['Totale_Giorno [€]'] = df_spesa_oraria.iloc[:,1:df_spesa_oraria.shape[1]].sum(axis=1) #totale giornaliero somma dei costi orari
dimensione = df_spesa_oraria.shape[0]
vec_m = []
for i in range(0, dimensione, 1):
    data = df_spesa_oraria.iloc[i,0]
    vec_m.append(str(data)[4:6])
df_spesa_oraria['mese'] = vec_m
df_spesa_mensile = df_spesa_oraria.groupby(['mese'])['Totale_Giorno [€]'].sum()
dizionario = {'immessa': 'imm', 'prelevata': 'prel', 'pz_sici': 'SICI', 'pun': 'PUN', 'pz_nord': 'NORD'}

# ESPORTAZIONE DATI ELABORATI
file_path_finale = os.path.join(origin_path, selezione, 'spesa_energetica')
if os.path.exists(file_path_finale) != True: 
    os.mkdir(file_path_finale)
file_path_finale = os.path.join(file_path_finale, str(anno))
if os.path.exists(file_path_finale) != True: 
    os.mkdir(file_path_finale)
df_spesa_oraria.to_csv(file_path_finale+'/'+str(anno)+'_spesa_oraria_'+dizionario[tipo_prezzo]+'_'+dizionario[tipo_regime]+'.csv', index=False, decimal =',', sep=';')
df_spesa_mensile.to_csv(file_path_finale+'/'+str(anno)+'_spesa_mensile_'+dizionario[tipo_prezzo]+'_'+dizionario[tipo_regime]+'.csv', index=False, decimal =',', sep=';')
print('Fine Operazioni')