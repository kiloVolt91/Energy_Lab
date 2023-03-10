from init_path import*

import pandas as pd
import sys
import os
import numpy as np
import configparser

import mysql.connector
from mysql.connector import errorcode
import datetime

#Apertura del file init.ini
config = configparser.ConfigParser()
config.read(init_path)
origin_path = config['user']['origin_path_edis']
pod_totali = config['user']['pod_totali']
output_data_path = config['user']['output_data_path']

sql_user = config['sql_database_energia']['user']
sql_password = config['sql_database_energia']['password']
sql_host = config['sql_database_energia']['host']
sql_database = config['sql_database_energia']['database']

#DIZIONARI E LISTE
dict_registro = {
                 'attiva prelevata':'1.8.0', 
                 'attiva immessa':'2.8.0', 
                 'reattiva induttiva prelevata':'5.8.0', 
                 'reattiva induttiva immessa':'6.8.0',
                 'reattiva capacitiva immessa':'7.8.0',
                 'reattiva capacitiva prele0vata':'8.8.0'
                }

mesi_edis = ['gennaio', 'febbraio', 'marzo', 'aprile', 'maggio', 'giugno', 'luglio', 'agosto', 'settembre', 'ottobre', 'novembre', 'dicembre']  #Vettore dei nomi contenuti nei file di e-distribuzione
giorni_mese = {'gennaio':31, 'febbraio':28, 'febbraio_bisestile':29, 'marzo':31, 'aprile':30, 'maggio':31, 'giugno':30, 'luglio':31, 'agosto':31, 'settembre':30, 'ottobre':31, 'novembre':30, 'dicembre':31} #dizionario contenente il numero di giorni per ciascun mese
numero_mese = {'gennaio':'01', 'febbraio':'02', 'febbraio_bisestile':'02', 'marzo':'03', 'aprile':'04', 'maggio':'05', 'giugno':'06', 'luglio':'07', 'agosto':'08', 'settembre':'09', 'ottobre':'10', 'novembre':'11', 'dicembre':'12'} #dizionario di associazione mese:numero_del_mese

#Creazione del dizionario contenente i pod e la relativa descrizione - vedi file init.ini
#N.B. inserire all'interno del file tutti i POD disponibili e la relativa descrizione
dizionario_pod = {}
for i in range (1,int(pod_totali)+1):
    dizionario_pod[config.get('POD', 'p'+str(i))] = config.get('utenza', 'u'+str(i))

#Creazione della lista contenente i nomi delle colonne del dataframe (i nomi delle colonne non sono arbitrari e dipendono dal file scaricato)
columns = np.array(
    list(
        config['dati_dataframe_edis']['colonne'].split(",")
    )   
)
#FINE INIZIALIZZAZIONE
