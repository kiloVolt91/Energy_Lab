## SCRIPT DI ESTRAZIONE AUTOMATICA DEI DATI DA FILE .CSV SCARICATO DAL PORTALE E-DISTRIBUZIONE.
## ASSICURARSI DI SCARICARE I FILE CON IL NOME CORRETTO ED INSERIRLI ALL'INTERNO DELLA CARTELLA:
## "curve_edis/IT001E_POD_UNIVOCO/ANNO/TIPOLOGIA DI ENERGA"

#INIZIALIZZAZIONE
import pandas as pd
import os
import numpy as np
import configparser
from init import init_path

#N.B. verificare la correttezza di tutti i percorsi

#apertura del file init.ini
config = configparser.ConfigParser()
config.read(init_path)
origin_path = config['user']['origin_path_edis']
pod_totali = config['user']['pod_totali']
output_data_path = config['user']['output_data_path']

#creazione del dizionario contenente i pod e la relativa descrizione - vedi file init.ini
#N.B. inserire all'interno del dizionario tutti i POD disponibili e la relativa descrizione
dizionario_pod = {}
for i in range (1,int(pod_totali)+1):
    dizionario_pod[config.get('POD', 'p'+str(i))] = config.get('utenza', 'u'+str(i))

#tolleranza di calcolo numerico - errore accettabile - verificare origine degli errori di calcolo numerico    
toll = float(1e-6) 

#creazione della lista contenente i nomi delle colonne del dataframe
colonne = list(  config['dati_dataframe_edis']['colonne'].split(",")  )
columns = np.array(colonne)

#Inizializzazione dataframe dei dati enel a 15 minuti
df_edis = pd.DataFrame(columns = columns) 
os.chdir(origin_path)

#Ciclo for per scansionare i nomi dei POD presenti nella cartella "curve e_dis". I nomi vengono inseriti all'interno del vettore "pod"
#ELENCO DELLE CARTELLE POD DISPONIBILI
pod = []   
for name in os.listdir(origin_path):    
    if name[0:6] =='IT001E':
        pod.append(name[0:18])
print('POD disponibili: ', pod)

#SELEZIONE DELLA CARTELLA POD 
i_pod = int(input('Inserisci il numero di posizione del POD desiderato dalla lista: ')) # input numerico 1...n corrispondente con l'indice i-1 del vettore pod
selezione = pod[i_pod-1]
print('Hai selezionato il seguente POD: ', selezione, dizionario_pod[selezione])
os.chdir(selezione)  #entro nella cartella del POD selezionato

#SELEZIONE DELLA CARTELLA ANNO
mesi_edis = ['gennaio', 'febbraio', 'marzo', 'aprile', 'maggio', 'giugno', 'luglio', 'agosto', 'settembre', 'ottobre', 'novembre', 'dicembre']  #Vettore dei nomi contenuti nei file di e-distribuzione
giorni_mese = {'gennaio':31, 'febbraio':28, 'febbraio_bisestile':29, 'marzo':31, 'aprile':30, 'maggio':31, 'giugno':30, 'luglio':31, 'agosto':31, 'settembre':30, 'ottobre':31, 'novembre':30, 'dicembre':31} #dizionario contenente il numero di giorni per ciascun mese
numero_mese = {'gennaio':'01', 'febbraio':'02', 'febbraio_bisestile':'02', 'marzo':'03', 'aprile':'04', 'maggio':'05', 'giugno':'06', 'luglio':'07', 'agosto':'08', 'settembre':'09', 'ottobre':'10', 'novembre':'11', 'dicembre':'12'} #dizionario di associazione mese:numero_del_mese
anno = input("Inserisci l'anno di riferimento in formato AAAA: \n") #selezione anno 

#Verifica di bisestilità dell'anno scelto, la verifica non è valida per gli anni bisestili secolari (divisibili per 100 e 400)
#Aggiungere verifica per anni secolari
if int(anno) % 4 == 0:
    febbraio = 'febbraio_bisestile' #anno bisestile
else:
    febbraio = 'febbraio' #anno non bisestile
mesi = ['gennaio', febbraio, 'marzo', 'aprile', 'maggio', 'giugno', 'luglio', 'agosto', 'settembre', 'ottobre', 'novembre', 'dicembre'] #vettore dei mesi con il mese di febbraio in dettaglio
os.chdir(anno)

#SELEZIONE DELLA CARTELLA ENERGETICA 
#Aggiungere dei controlli
tipo_energia = int(input("Tipologia di energia: inserisci 1 per l'energia attiva, inserisci 2 per l'energia reattiva induttiva, inserisci 3 per l'energia reattiva capacitiva: \n"))
tipo_regime = int(input("Tipologia di regime di cessione: inserisci 1 per l'immissione di energia, 2 per il prelievo di energia: \n"))

if (tipo_energia >=1) & (tipo_energia<=3):
    if tipo_energia == 1:
        tipo_energia = 'attiva'
    if tipo_energia == 2:
        tipo_energia = 'reattiva induttiva'
    if tipo_energia == 3:
        tipo_energia = 'reattiva capacitiva'
else:
    print('tipo energia: è stato inserito un valore errato')

if (tipo_regime >=1) & (tipo_regime<=2):
    if tipo_regime == 1:
        tipo_regime = 'immessa'
    if tipo_regime == 2:
        tipo_regime = 'prelevata'
else:
    print('tipo regime: è stato inserito un valore errato')    
os.chdir(tipo_energia+' '+tipo_regime)
cwd = os.getcwd()


#ESTRAZIONE DEI DATI E-DISTRIBUZIONE
for m in range (0,12):    
    if os.path.exists(cwd+'/ExportData_' + mesi_edis[m] + '.csv') == True:     
        df_month = pd.read_csv('ExportData_' + mesi_edis[m] + '.csv', sep=';', index_col=False, decimal=",") #dati del file mensile di riferimento
        df_edis = pd.concat([df_edis,df_month])
    else: #nel caso in cui non sia stato trovato alcun file per il mese selezionato, lo spazio lasciato vuoto nel dataframe viene riempito di valori "NaN"
        nan_vec = [np.nan]* giorni_mese[mesi[m]]  #vettore contenente n valori NaN tanti quanti i giorni del mese di riferimento
        df_month = pd.DataFrame()

        #CREAZIONE DELLA COLONNA DEL DATAFRAME CONTENENTE LE DATE DA ASSOCIARE AI VALORI NAN
        ind_mese = numero_mese[mesi[m]]
        day_vec = []
        for i in range(1,giorni_mese[mesi[m]]+1):
            day_vec.append(str(i).zfill(2)+'/'+ind_mese+'/'+anno)          
        df_month['Giorno'] = day_vec 
        #CREAZIONE DELLA COLONNA CONTENENTE I VALORI NAN
        for j in range (1, len(columns)):
            df_month[columns[j]] = pd.DataFrame(nan_vec)
        df_edis = pd.concat([df_edis,df_month])      
        print('Dati assenti per il seguente mese: ' + mesi_edis[m])
os.chdir(origin_path)        
        
#MODIFICHE ALLA STRUTTURA DEL DATAFRAME
df_edis.reset_index(inplace=True) 
df_edis.drop(columns='index', inplace=True)
nomi_colonne = list(df_edis.head(0))

df_orario = df_edis['Giorno'].copy()
headers_orari = ['Giorno \\ Ora']

#VERIFICA PRE-MANIPOLAZIONE DEL VALORE NUMERICO DEI DATI GREZZI
check_iniziale = df_edis.iloc[:,1:97].astype(float).sum().sum() 
print('ENERGIA PRE-MANIPOLAZIONE: ')
print(check_iniziale)

#DATAFRAME ORARIO DELL'ENERGIA - CREAZIONE DI RAGGRUPPAMENTI PER CIASCUNA ORA DEL GIORNO DEI DATI A 15 MINUTI DI ENEL
for k in range(0,24,1):
    headers_orari.append(k+1)
    df_sum = df_edis[nomi_colonne[4*k+1]].astype(float)+df_edis[nomi_colonne[4*k+2]].astype(float)+df_edis[nomi_colonne[4*k+3]].astype(float)+df_edis[nomi_colonne[4*k+4]].astype(float) #RAGGRUPPAMENTO PER MULTIPLI DI 4 SULLE COLONNE (4 QUARTI D'ORA)
    df_orario=pd.concat([df_orario,df_sum], axis=1)
df_orario.set_axis(headers_orari,axis=1, inplace=True)
df_orario['Totale_Giorno'] = df_orario.iloc[:,1:25].sum(axis=1) #SOMMA DELL'ENENERGIA DI CIASCUNA ORA PER AVERE IL TOTALE GIORNALIERO
Energia_annua = df_orario['Totale_Giorno'].sum() #SOMMA DELLE ENERGIE GIORNALIERE PER OTTENERE IL TOTALE ANNUO
print('ENERGIA ANNO: ')
print(Energia_annua)

#INDIVIDUAZIONE DEL MESE DI RIFERIMENTO PER CIASCUN GIORNO - step necessario per l'estrazione del dataframe di riepilogo mensile
dimensione = df_orario.shape[0]
vec_m = []
for i in range(0, dimensione, 1):
    data = df_orario.iloc[i,0]
    vec_m.append(data[3:5])          
df_orario['mese'] = vec_m

##CORREZIONE ORA LEGALE-ORA SOLARE - Creazione dell'ora n.25 che conterrà un solo valore
df = df_orario.copy()
df.drop(columns=['Totale_Giorno', 'mese'], inplace=True)
empty = [np.nan]*df_orario.shape[0]
df['25'] = empty
drop=[]
ora_switch = 3 #il cambio di orario avviene tra le 2:00 e le 3:00 nel caso di ora solare a marzo, l'ora viene portata indietro
#in caso di ora legale, l'ora viene portata avanti

for i in range(1, df.shape[0]):
    a = str(df[headers_orari].iloc[i-1,0])
    b = str(df[headers_orari].iloc[i,0])
    if a == b:  #verifico l'esistenza di date identiche coincidenti con ora solare e legale        
        mese = int(df[headers_orari].iloc[i-1,0][3:5])     
        if mese == 3: #ORA SOLARE devo incollare il vettore i a partire dalla colonna 3 in posizione i-1 a partire dalla colonna 2
            st = df.iloc[i,1:df.shape[1]].sum() + df.iloc[i-1,1:df.shape[1]].sum()        
            for j in range (ora_switch, df.shape[1]-1):
                df.iloc[i,j] = df.iloc[i-1,j]
            df.iloc[i-1,24] = np.nan
            drop.append(i-1)
            sf = df.iloc[i,1:df.shape[1]].sum()

#ORA LEGALE incollare il vettore i a partire dalla colonna 3 ed in posizione i-1 a partire dalla colonna 4
            
        elif mese == 10:
            st = df.iloc[i,1:df.shape[1]].sum() + df.iloc[i-1,1:df.shape[1]].sum()      
            for j in range (ora_switch, df.shape[1]-1):
                df.iloc[i-1,j+1] = df.iloc[i,j]
            drop.append(i)
            sf = df.iloc[i-1,1:df.shape[1]].sum()
        else:
            print('Anomalia nei dati nel mese...')
            print(mese)
                
if len(drop)>2:
    print('VERIFICARE ANOMALIA DATE')
                
for i in range (0, len(drop)):
    df = df.drop([drop[i]])

# VERIFICA INTEGRITA' DEI DATI MANIPOLATI
check_finale = df.iloc[:,1:df.shape[1]].sum().sum() 

if check_finale - check_iniziale > toll:
    print ("L'energia iniziale non corrisponde con quella finale")
else:
    print('Esito Positivo')

#CREAZIONE E COLLOCAZIONE DEL FILE .CSV CONTENENTE I DATI MANIPOLATI ED ORDINATI
data_path = os.path.join(output_data_path, 'dati_elaborati', selezione )
if os.path.exists(data_path) != True: 
    os.mkdir(data_path)

os.chdir(data_path)
data_path = os.path.join(data_path, 'e-dis')

if os.path.exists(data_path) != True: 
    os.mkdir(data_path)
    
os.chdir(data_path)
data_path = os.path.join(data_path, anno)

if os.path.exists(data_path) != True: 
    os.mkdir(data_path)
    
export_path = os.path.join(data_path, 'riepilogo_15min_'+ tipo_energia + '_' + tipo_regime + '.csv')
df_edis.to_csv(export_path, index=False, decimal =',', sep=';')
export_path = os.path.join(data_path, 'riepilogo_orario_'+ tipo_energia + '_' + tipo_regime + '.csv')
df.to_csv(export_path, index=False, decimal =',', sep=';')

#CREAZIONE DATAFRAME MENSILE
df_mensile = df_orario.groupby(['mese'])['Totale_Giorno'].sum()
export_path = os.path.join(data_path, 'riepilogo_mensile_'+ tipo_energia + '_' + tipo_regime + '.csv')
df_mensile.to_csv(export_path, index=False, decimal =',', sep=';')

print ('Fine Operazione')