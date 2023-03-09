## SCRIPT DI ESTRAZIONE AUTOMATICA DEI DATI DA FILE .CSV SCARICATO DAL PORTALE E-DISTRIBUZIONE.
## ASSICURARSI DI SCARICARE I FILE CON IL NOME CORRETTO ED INSERIRLI ALL'INTERNO DELLA CARTELLA:
## "curve_edis/IT001E_POD_UNIVOCO/ANNO/TIPOLOGIA DI ENERGIA"
#N.B. verificare la correttezza di tutti i percorsi

#INIZIALIZZAZIONE

from init import * #Posizione del file init.ini 
from funzioni import *

#ELENCO DELLE CARTELLE POD DISPONIBILI
lista_pod = []   
for name in os.listdir(origin_path):    
    if name[0:6] =='IT001E':
        lista_pod.append(name[0:18])
if lista_pod:
    print('POD disponibili: ')
    for pod in lista_pod:
        print(lista_pod.index(pod)+1,']  ', pod, dizionario_pod[pod])
else:
    sys.exit('Nessun POD disponibile nella cartella indicata: '+ str(origin_path))

#SELEZIONE DELLA CARTELLA POD 
while True:
    os.chdir(origin_path)
    i_pod = input('Inserisci il numero di posizione del POD desiderato dalla lista: ') # input numerico 1...n corrispondente con l'indice i-1 del vettore pod
    if i_pod.isnumeric() == False:
        sys.exit('Il valore inserito non è un numero')
    if int(i_pod) > len(lista_pod):
        sys.exit('Il valore inserito eccede i POD disponibili')
    selezione_pod = lista_pod[int(i_pod)-1]    
    print('Hai selezionato il seguente POD: ', selezione_pod, dizionario_pod[selezione_pod])
    os.chdir(selezione_pod)  #entro nella cartella del POD selezionato
    break

#SELEZIONE DELL'ANNO  
lista_anni = []   
for name in os.listdir(os.getcwd()):    
    if (name[0:2] =='20') or (name[0:2] =='19'):
        lista_anni.append(name[0:18])
if lista_anni:
    print('Anni disponibili: ')
    for anno in lista_anni:
        print(lista_anni.index(anno)+1,']  ', anno)
else:
    sys.exit('Nessun anno disponibile nella cartella indicata: '+ str(origin_path)) 
while True:
    i_anno = input("Seleziona l'anno di riferimento: \n") #selezione anno  
    if i_anno.isnumeric() == False:
        sys.exit('Il valore inserito non è un numero')
    if int(i_anno) > len(lista_anni):
        sys.exit('Il valore inserito eccede gli anni disponibili')
    selezione_anno = lista_anni[int(i_anno)-1]    
    print('Hai selezionato il seguente anno: ', selezione_anno)
    os.chdir(selezione_anno)  #entro nella cartella del POD selezionato
    break

#VERIFICA BISESTILITÀ
if (int(selezione_anno)%4 == 0 and int(selezione_anno)%100 !=0) or (int(selezione_anno)%400 == 0):
    febbraio = 'febbraio_bisestile' #anno bisestile
else:
    febbraio = 'febbraio' #anno non bisestile    
mesi = ['gennaio', febbraio, 'marzo', 'aprile', 'maggio', 'giugno', 'luglio', 'agosto', 'settembre', 'ottobre', 'novembre', 'dicembre'] #vettore dei mesi con il mese di febbraio in dettaglio

#SELEZIONE DELLA TIPOLOGIA DI ENERGIA E DI REGIME
while True:
    tipo_energia = input("Tipologia di energia:\n 1) Energia Attiva\n 2) Energia Reattiva Induttiva\n 3) Energia Reattiva Capacitiva \n")
    if tipo_energia.isnumeric() == False:
        sys.exit('Il valore inserito non è un numero')
    if int(tipo_energia) > 3:
        sys.exit('Il valore inserito non è in lista')
    while True:
        if int(tipo_energia) == 1:
            tipo_energia = 'attiva'
            break
        if int(tipo_energia) == 2:
            tipo_energia = 'reattiva induttiva'
            break
        if int(tipo_energia) == 3:
            tipo_energia = 'reattiva capacitiva'
            break
    print('Hai selezionato la seguente tipologia: ', tipo_energia)
    break    
while True:
    tipo_regime = input("Tipologia di regime di cessione:\n 1) Immissione di Energia\n 2) Prelievo di Energia\n")
    if tipo_regime.isnumeric() == False:
        sys.exit('Il valore inserito non è un numero')
    if int(tipo_regime) > 2:
        sys.exit('Il valore inserito non è in lista')
    while True:
        if int(tipo_regime) == 1:
            tipo_regime = 'immessa'
            break
        if int(tipo_regime) == 2:
            tipo_regime = 'prelevata'
            break
    print('Hai selezionato il seguente regime: ', tipo_regime)
    break
os.chdir(tipo_energia+' '+tipo_regime)
cwd = os.getcwd()

#IMPORTAZIONE DEI DATI E-DISTRIBUZIONE IN UN DATAFRAME
#I DATI SONO MENSILI CON INTERVALLO DI CAMPIONAMENTO PARI A 15 MINUTI
df_edis_15m = pd.DataFrame(columns = columns) 
for mese in mesi_edis:    
    if os.path.exists(cwd+'/ExportData_' + mese + '.csv') == True:     
        df_singolo_mese = pd.read_csv('ExportData_' + mese + '.csv', sep=';', index_col=False, decimal=",") #dati del file mensile di riferimento
        df_edis_15m = pd.concat([df_edis_15m,df_singolo_mese])
    else: 
        #nel caso in cui non sia stato trovato alcun file per il mese selezionato, lo spazio lasciato vuoto nel dataframe viene riempito di valori "NaN"
        nan_vec = [np.nan]* giorni_mese[mese]  #vettore contenente n valori NaN tanti quanti i giorni del mese di riferimento
        df_singolo_mese = pd.DataFrame() # inizializzazione df
        #CREAZIONE DELLA COLONNA DEL DATAFRAME CONTENENTE LE DATE DA ASSOCIARE AI VALORI NAN
        indice_mese = numero_mese[mese]
        day_list = []
        for i in range(1,giorni_mese[mese]+1):
            day_list.append(str(i).zfill(2)+'/'+indice_mese+'/'+anno)          
        df_singolo_mese['Giorno'] = day_list 
        #CREAZIONE DELLA COLONNA CONTENENTE I VALORI NAN
        for j in range (1, len(columns)):
            df_singolo_mese[columns[j]] = pd.DataFrame(nan_vec)
        df_edis_15m = pd.concat([df_edis_15m,df_singolo_mese])      
        print('Dati assenti per il seguente mese: ' + mesi_edis[mesi_edis.index(mese)])
df_edis_15m.reset_index(inplace=True) 
df_edis_15m.drop(columns='index', inplace=True)

os.chdir(origin_path)        

#CREAZIONE DI ALTRI DATAFRAME - DETTAGLIO ORARIO (UTILE PER GME) E DETTAGLIO MENSILE (UTILE PER DICHIARAZIONI E VERIFICHE)
df_edis_1h = raggruppamento_orario_dataframe(df_edis_15m)
verifica_integrita_df (df_edis_15m, df_edis_1h)
df_edis_mese = creazione_riepilogo_mensile(df_edis_15m)

#ESPORTAZIONE RIEPILOGHI .CSV
export_path = selezione_cartella_export_dati (output_data_path, selezione_pod, selezione_anno)
esporta_csv(df_edis_15m, 'riepilogo_15min_', export_path, tipo_energia, tipo_regime)
esporta_csv(df_edis_1h, 'riepilogo_orario_', export_path, tipo_energia, tipo_regime)
esporta_csv(df_edis_mese, 'riepilogo_mensile_', export_path, tipo_energia, tipo_regime)

print('Fine Operazioni')