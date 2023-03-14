from init import *
def sql_export_df(df, selezione_pod, tipo_energia, tipo_regime, sql_tabella):
    try:
        cnx = mysql.connector.connect(user=sql_user, password=sql_password, host=sql_host, database=sql_database)  

    except mysql.connector.Error as err:
      if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("Something is wrong with your user name or password")
      elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("Database does not exist")
      else:
        print(err) 

    #AGGIUNTA DI INFORMAZIONI
    pod_list = [selezione_pod]*df.shape[0]

    reg_val = str(dict_registro[tipo_energia + ' ' + tipo_regime])
    reg_list=[]
    for i in range (0,df.shape[0]):
        reg_list.append(reg_val)


    df['fk_pod'] = pod_list
    df['fk_registro'] = reg_list


    columns = df.columns  
    df.fillna(-1, inplace =True)

    placeholders = '%s'
    str_nomi = '('+columns[0]+','
    str_vals = '(%s,'
    for i in range(1, len(columns)):
        if i == len(columns)-1:
            str_nomi = str_nomi +'`'+ columns[i] +'`' +')'
            str_vals = str_vals + placeholders + ')'
        else: 
            str_nomi = str_nomi +'`'+ columns[i] +'`'+', '
            str_vals = str_vals + placeholders + ', '
    mysql_str = "INSERT INTO "+ sql_tabella+ " {col_name} VALUES {values}".format(col_name = str_nomi, values = str_vals)
    cursor = cnx.cursor()
    df['Giorno'] = pd.to_datetime(df['Giorno'].str.slice(0,10), format='%d/%m/%Y')

    for i in range (0, df.shape[0]):
        cursor.execute (mysql_str, df.iloc[i].tolist())
    cnx.commit()
    cursor.close()
    cnx.close()
    return

def sql_export_mese(df_edis_mese, selezione_pod, tipo_energia, tipo_regime, selezione_anno):
    try:
        cnx = mysql.connector.connect(user=sql_user, password=sql_password, host=sql_host, database=sql_database)  

    except mysql.connector.Error as err:
      if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("Something is wrong with your user name or password")
      elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("Database does not exist")
      else:
        print(err) 

    #AGGIUNTA DI INFORMAZIONI
    pod_list = [selezione_pod]*df_edis_mese.shape[0]

    reg_val = str(dict_registro[tipo_energia + ' ' + tipo_regime])
    reg_list=[]
    for i in range (0,df_edis_mese.shape[0]):
        reg_list.append(reg_val)


    df_edis_mese['fk_pod'] = pod_list
    df_edis_mese['fk_registro'] = reg_list
    df_edis_mese['fk_anno'] = selezione_anno

    columns = df_edis_mese.columns  
    df_edis_mese.fillna(-1, inplace =True)

    placeholders = '%s'
    str_nomi = '('+columns[0]+','
    str_vals = '(%s,'
    for i in range(1, len(columns)):
        if i == len(columns)-1:
            str_nomi = str_nomi +'`'+ columns[i] +'`' +')'
            str_vals = str_vals + placeholders + ')'
        else: 
            str_nomi = str_nomi +'`'+ columns[i] +'`'+', '
            str_vals = str_vals + placeholders + ', '
    mysql_str = "INSERT INTO edis_mese {col_name} VALUES {values}".format(col_name = str_nomi, values = str_vals)

    cursor = cnx.cursor()
    

    for i in range (0, df_edis_mese.shape[0]):
        cursor.execute (mysql_str, df_edis_mese.iloc[i].tolist())
    cnx.commit()
    cursor.close()
    cnx.close()
    return