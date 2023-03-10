#FUNZIONI
from init import*

def raggruppamento_orario_dataframe(df):
    nomi_colonne = list(df.columns)
    df_orario = df['Giorno'].copy()
    headers_orari = ['Giorno']
    for k in range(0,24,1):
        headers_orari.append('ora ' + str(k+1))
        #RAGGRUPPAMENTO PER MULTIPLI DI 4 SULLE COLONNE (4 QUARTI D'ORA)
        df_sum_iter = df[nomi_colonne[4*k+1]].astype(float)+df[nomi_colonne[4*k+2]].astype(float)+df[nomi_colonne[4*k+3]].astype(float)+df[nomi_colonne[4*k+4]].astype(float)
        df_orario = pd.concat([df_orario,df_sum_iter], axis=1)
    df_orario.set_axis(headers_orari,axis=1, inplace=True)
    
    df_orario = correzione_switch_ora_solare(df_orario)
    
    return (df_orario)

def correzione_switch_ora_solare(df):
    toll = 1e-06
    headers_orari = df.columns
    empty = [np.nan]*df.shape[0]
    df['ora 25'] = empty
    drop_list=[]
    ora_switch = 3 
    for i in range(1, df.shape[0]):
        a = str(df[headers_orari].iloc[i-1,0])
        b = str(df[headers_orari].iloc[i,0])
        if a == b:  # Verifica dell'esistenza di date coincidenti su più righe del DF
            mese = int(df[headers_orari].iloc[i-1,0][3:5]) #Estrazione dal valore 'str' il valore associato al mese 
            
            #ORA SOLARE devo incollare il vettore i a partire dalla colonna 3 in posizione i-1 a partire dalla colonna 2
            if mese == 3: 
                drop_list.append(i-1)
                st = df.iloc[i,1:df.shape[1]].astype(float).sum() + df.iloc[i-1,1:df.shape[1]].astype(float).sum() #Verifica pre-manipolazione - somma dei valori nelle due righe    
                for j in range (ora_switch, df.shape[1]-1): #Traslazione delle righe 
                    df.iloc[i,j] = df.iloc[i-1,j]
                df.iloc[i-1,24] = np.nan
                sf = df.iloc[i,1:df.shape[1]].astype(float).sum()
                if abs(st - sf) > toll:
                    print(sf, st,'Mese: ', mese)
                    sys.exit('Errore nella correzione ora solare')       
                    
            #ORA LEGALE incollare il vettore i a partire dalla colonna 3 ed in posizione i-1 a partire dalla colonna 4
            elif mese == 10:
                drop_list.append(i)
                st = df.iloc[i,1:df.shape[1]].astype(float).sum() + df.iloc[i-1,1:df.shape[1]].astype(float).sum()
                for j in range (ora_switch, df.shape[1]-1): #Traslazione delle righe 
                    df.iloc[i-1,j+1] = df.iloc[i,j]
                sf = df.iloc[i-1,1:df.shape[1]].astype(float).sum()
                if abs(st - sf) > toll:
                    print(sf, st, 'Mese: ', mese)
                    sys.exit('Errore nella correzione ora solare')  
                    
            else:
                print('Anomalia nei dati nel mese...')
                print(mese)  
                sys.exit('Verificare anomalia dati')
                
    if len(drop_list)>2:
        sys.exit('Verificare anomalia dati - date coincidenti > 2')
    for i in range (0, len(drop_list)):
        df = df.drop([drop_list[i]])                           
    return(df)


def verifica_integrita_df(df_iniziale, df_finale):
    toll = 1e-06
    check_iniziale = df_iniziale.iloc[:,1:df_iniziale.shape[1]].astype(float).sum().sum()
    check_finale = df_finale.iloc[:,1:df_finale.shape[1]].astype(float).sum().sum()
    print('Energia Totale Iniziale: ', check_iniziale, '\n','Energia Totale Finale: ', check_finale)
    if abs(check_iniziale - check_finale) > toll:
        sys.exit("L'energia iniziale non coincide con quella finale")
    print('La verifica ha avuto esito positivo')
    return 

def creazione_riepilogo_mensile(df):
    df_mensile = df.copy()
    df_mensile.fillna(value=0.0, inplace=True)
    lista_mesi = []
    for i in range(0, df_mensile.shape[0]):
        str_mese = df_mensile.iloc[i,0]
        lista_mesi.append(str_mese[3:5])          
    df_mensile['Totale giornaliero'] = df_mensile.iloc[:,1:df_mensile.shape[1]].astype(float).sum(axis=1)

    df_mensile['mese'] = lista_mesi
    df_mensile = df_mensile.groupby(['mese'])['Totale giornaliero'].sum().to_frame()
    df_mensile.reset_index(inplace=True)
    
    verifica_integrita_df(df, df_mensile)
    
    return(df_mensile)

def crea_percorso (directory):
    cwd = os.getcwd()
    data_path = os.path.join(cwd, directory)
    if os.path.exists(data_path) != True: 
        os.mkdir(data_path) 
    os.chdir(data_path)
    return

def selezione_cartella_export_dati (output_data_path, selezione_pod, selezione_anno):
    os.chdir(output_data_path)
    
    crea_percorso('dati_elaborati')
    crea_percorso(selezione_pod)
    crea_percorso('e-dis')
    crea_percorso(selezione_anno)
    
    export_path = os.getcwd()
    return (export_path)

def esporta_csv(df, nome_file, export_path, tipo_energia, tipo_regime):
    export_path = os.path.join(export_path, nome_file + tipo_energia + '_' + tipo_regime + '.csv')
    df.to_csv(export_path, index=False, decimal =',', sep=';')
    return