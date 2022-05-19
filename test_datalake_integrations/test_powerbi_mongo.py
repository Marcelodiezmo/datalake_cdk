# -*- coding: utf-8 -*-
"""
Created on Mon Apr 11 16:18:53 2022

@author: Ubits
"""
import pymongo
import skynetmodule as skm
def test_powerbi_mongo_conn():
    modeEnv='DEV'
    if modeEnv == 'DEV':
        conn_str = "mongodb://Jarvis:Jarvis123@10.0.5.144:27017/PowerBIInternal?retryWrites=true&w=majority" #DEV
    else:
        conn_str = "mongodb://Jarvis:HAqsuoLeBTrj@10.0.5.206:27017/PowerBIInternal?retryWrites=true&w=majority"   #PROD
    client = pymongo.MongoClient(conn_str, serverSelectionTimeoutMS=5000)     
    assert "version" in client.server_info()
        
    
    
def test_powerbi_mongo_insert():
    modeEnv='DEV'
    if modeEnv == 'DEV':
        conn_str = "mongodb://Jarvis:Jarvis123@10.0.5.144:27017/PowerBIInternal?retryWrites=true&w=majority"   #DEV
    else:
        conn_str = "mongodb://Jarvis:HAqsuoLeBTrj@10.0.5.206:27017/PowerBIInternal?retryWrites=true&w=majority"  # PROD
        
    client = pymongo.MongoClient(conn_str, serverSelectionTimeoutMS=5000) 
    db = client["PowerBIInternal"]
    #dbUsersCompanyPowerBI = client['PowerBIInternal']['usersCompanyPowerBI']
    #dbcompanies = client['PowerBIInternal']['companies']
    #dbareasEmpresa = client['PowerBIInternal']['areasCompany']
    #bucket = 'analytics-ubits-production-dev'
    bucket = 'analytics-ubits-production'
    #modeEnv = "DEV"
    modeEnv = "PROD"
    keyId, keyAccess = skm.get_default_credentials(modeEnv)
    #region = 'us-east-1'
    
    #Usuarios
    filepathUsers = 'APIs/update_users_analitics.parquet'
    usuarios_insert = skm.pd_read_s3_multiple_parquets(aws_id=keyId,aws_secret=keyAccess,filepath=filepathUsers,bucket=bucket)
    usuarios_insert=usuarios_insert.sample(1)
    usuarios_insert=usuarios_insert['idUsuario'].values[0]
    
    #Company
    filepathCompanies = 'APIs/companies.parquet'
    empresas = skm.pd_read_s3_multiple_parquets(aws_id=keyId,aws_secret=keyAccess,filepath=filepathCompanies,bucket=bucket)
    empresas=empresas.sample(1)
    empresas=empresas['id'].values[0]
    
    ## Areas X Empresa
    filepathAreasCompany = 'APIs/areasCompany.parquet'
    areasEmpresa = skm.pd_read_s3_multiple_parquets(aws_id=keyId,aws_secret=keyAccess,filepath=filepathAreasCompany,bucket=bucket)
    areasEmpresa=areasEmpresa.sample(1)  #escoger una fila del dataframe al azar
    areasEmpresa=areasEmpresa['id'].values[0] 
    
    
    data_usersCompanyPowerBI=[]
    for x in db.usersCompanyPowerBI.find({"idUsuario":int(usuarios_insert)}):   
        data_usersCompanyPowerBI.append(x)
    
    
    data_companies=[]
    for x in db.companies.find({"id":empresas}):
        data_companies.append(x)
    
    
    
    areasCompany=[]
    for x in db.areasCompany.find({"id":areasEmpresa}):
        areasCompany.append(x)
    
     
    assert len(data_usersCompanyPowerBI)>=1  #validar si estan
    assert len(data_companies)>=1 
    assert len(areasCompany)>=1 
    