import pandas as pd
import numpy as numpy
import os
from datetime import date


def run():
    path = "D:/Notas de Estudio/Proyectos/calculate_diff/Calc_diff/data/raw"
    files = []

    for (dirpath, dirnames, filenames) in os.walk(path):       
        files.extend(filenames)
    
    print(files)
    print('\n')

    print('Loading Data...')
    print('\n')
    df_data = pd.read_excel(path+'/'+files[1], sheet_name="Prod. EXTR",header=0)
    df_db = pd.read_excel(path+'/'+files[2],header=0)

    print('Transforming Data...')
    print('\n')
    print('df_r_w')
    print('\n')
    df_r_w = df_data[['MÁQ,','CODIGO','GRUPO','PRODUCCION (PZAS)','TOTAL KG']]
    df_r_w.dropna(subset = ['MÁQ,','CODIGO','PRODUCCION (PZAS)','TOTAL KG'],inplace =True)
    rows_a= []
    a = df_r_w['CODIGO'].apply(lambda x: rows_a.append(x) if x.startswith('BOAB') else None)
    rows_a = list(set(rows_a))
    rows_b = []
    for row in rows_a:
        row = row.replace(',','.') 
        rows_b.append(row)
    df_r_w['CODIGO'].replace(rows_a,rows_b,inplace=True )
    df_r_w.rename(columns = {'TOTAL KG' : 'KG_Sistema'}, inplace = True)
    print('df_db')
    print('\n')
    df_db.dropna(inplace = True)
    print('Join DataFrames')
    print('\n')
    df_r_w.reset_index()
    df = pd.merge(df_r_w, df_db, how="inner", on=["CODIGO"])
    print('Transforming Final DataFrames')
    print('\n')
    df['KG_Teorico'] = df['T_W (kg)'] * df['PRODUCCION (PZAS)']
    df['DIFF'] = df['KG_Sistema'] - df['KG_Teorico']
    # change columns data types
    df['PRODUCCION (PZAS)'] = df['PRODUCCION (PZAS)'].astype('int32')
    df['KG_Sistema'] = df['KG_Sistema'].astype('float32').round(1)
    df['T_W (kg)'] = df['T_W (kg)'].astype('float32').round(1)
    df['KG_Teorico'] = df['KG_Teorico'].astype('float32').round(1)
    df['DIFF'] = df['DIFF'].astype('float32').round(1)
    # continue transforming data
    df_diff = df.groupby(by=['MÁQ,','CODIGO','GRUPO']).agg({'KG_Sistema':'sum','KG_Teorico':'sum','DIFF':'sum'})
    df_diff_gen = df.groupby(by = ['MÁQ,']).agg({'DIFF':'sum'})
    # difference per day
    df_day = df_data[['FECHA','MÁQ,','CODIGO','GRUPO','PRODUCCION (PZAS)','TOTAL KG']]
    df_day.dropna(axis = 0,how='any', inplace=True)
    df_day['FECHA'] = pd.to_datetime(df_day['FECHA'], format= '%Y-%m-%d')
    df_day['FECHA'].dt.day 
    df_day['DIA'] = df_day['FECHA'].dt.day
    #yesterday = date.today().day
    yesterday = 24
    df_day = df_day[df_day['DIA'] == yesterday]
    df_day.drop(labels =['FECHA','DIA'], axis=1, inplace=True)
    rows_a= []
    a = df_day['CODIGO'].apply(lambda x: rows_a.append(x) if x.startswith('BOAB') else None)
    rows_a = list(set(rows_a))
    rows_b = []
    for row in rows_a:
        row = row.replace(',','.') 
        rows_b.append(row)
    df_day['CODIGO'].replace(rows_a,rows_b,inplace=True )
    df_day.rename(columns = {'TOTAL KG' : 'KG_Sistema'}, inplace = True)
    df_day.reset_index()
    df_day = pd.merge(df_day, df_db, how="inner", on=["CODIGO"])
    df_day['KG_Teorico'] = df_day['T_W (kg)'] * df_day['PRODUCCION (PZAS)']
    df_day['DIFF'] = df_day['KG_Sistema'] - df_day['KG_Teorico']
    df_day['PRODUCCION (PZAS)'] = df_day['PRODUCCION (PZAS)'].astype('int32')
    df_day['KG_Sistema'] = df_day['KG_Sistema'].astype('float32').round(1)
    df_day['T_W (kg)'] = df_day['T_W (kg)'].astype('float32').round(1)
    df_day['KG_Teorico'] = df_day['KG_Teorico'].astype('float32').round(1)
    df_day['DIFF'] = df_day['DIFF'].astype('float32').round(1)
    df_diff_day = df_day.groupby(by=['MÁQ,','CODIGO','GRUPO']).agg({'KG_Sistema':'sum','KG_Teorico':'sum','DIFF':'sum'})


    #exportin data
    print('Exporting data')
    print('\n')
    path = "D:/Notas de Estudio/Proyectos/calculate_diff/Calc_diff/data/processed"
    os.chdir(path)
    with pd.ExcelWriter("diff.xlsx") as writer:
        df_diff_gen.to_excel(writer, sheet_name="Sheet1")
        df_diff.to_excel(writer, sheet_name="Sheet2")
        df_diff_day.to_excel(writer,sheet_name='Sheet3')





if __name__ == '__main__':
    run()