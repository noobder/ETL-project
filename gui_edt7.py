# -*- coding: utf-8 -*-
"""
Created on Mon May  2 08:04:43 2022

@author: rahul
"""
#from typing_extensions import Self

from matplotlib.transforms import Transform
import pandas as pd
import numpy as np
import tkinter as tk
import os
from tkinter import filedialog
from tkinter import *
import mysql.connector
import tkinter.messagebox
import xml.etree.ElementTree as ET
import webbrowser as wb
from xml.dom import minidom, Node
from tkinter import messagebox as mb
from sqlalchemy import create_engine
from dateutil.parser import parse
from sklearn import preprocessing
import io
from flask import Flask, render_template
import webbrowser
import datetime 
  


# user-defined packages


#Reading form xml file
dbdoc = ET.parse('dbandpath.xml')
for d in dbdoc.iterfind('DBbody'):
    db_username = d.findtext('db_user')
    db_pwd = d.findtext('password')
    db_host = d.findtext('host')
    db_database = d.findtext('db_database')
    path = d.findtext('file_path')
    table1 = d.findtext('db_table1')
    table2 = d.findtext('db_table2')

#con = mysql.connector.connect(host='localhost',user='root',password='Root')

global df_g
global df_g2
dg_g = pd.DataFrame()
df_g = pd.DataFrame()
source_path = []
ext = None

def print_df_info(self,df_g):
    buffer = io.StringIO()
    df_g.info(buf=buffer)
    s = buffer.getvalue()
    with open("df_info.txt", "w",
              encoding="utf-8") as f:  # doctest: +SKIP
         f.write(s)     
    with open("df_info.txt", "r",
              encoding="utf-8") as f:
        s = f.read()
        s = s[37:]
        s = s[:-23]
    self.tbBox.insert(END,'\n'+s)




class Homepage:
    def __init__(self,master,*args,**kwargs):
        self.master=master
        master.title("Inventary Control System")
        self.heading=Label(master,text="Welcome to ETL SYSTEM",font=('arial 35 bold italic'),fg='teal')
        self.heading.place(x=400,y=0)
        #button for navigating to extract page
        self.btn_add=Button(master,text='EXTRACT',width=30,height=5,bg='steelblue',fg='white',command=self.hometoext)
        self.btn_add.place(x=170,y=200)
        #button for navigating to transform page
        #self.btn_add=Button(master,text='TRANSFORM',width=30,height=5,bg='steelblue',fg='white',command=self.hometotrans)
        #self.btn_add.place(x=170,y=270)
        #button for navigating to load page
        #self.btn_add=Button(master,text='LOAD',width=30,height=5,bg='steelblue',fg='white',command=self.hometoload)
        #self.btn_add.place(x=170,y=370)
        #button for navigating to dashboard
        self.btn_add=Button(master,text='DASHBOARD',width=30,height=5,bg='steelblue',fg='white',command=self.hometodash)
        self.btn_add.place(x=170,y=430)
        #text box for the log
        self.tbBox=Text(master,width=60,height=35)
        self.tbBox.place(x=750,y=75)
        self.tbBox.insert(END,"\n\n\t\tDialogue Box\n1.click on EXTRACT\n\t to import data into etl system\n2.cilck on TRANSFORM\n\t to perform operations on data\n3.click on LOAD\n\t to export the data into destination\n4.click on DASHBOARD\n\t to check status of records")
    
    def hometoext(self):
        for widget in self.master.winfo_children():
            widget.destroy()
        self.master=gotoExtraction(self.master)

    def hometodash(self):
        gotoDashboard()
    
                          
                   
#Extraction of data                 
class Extraction:
    def __init__(self,master,*args,**kwargs):
        self.master=master
        self.heading=Label(master,text="Extraction",font=('arial 35 bold italic'),fg='teal')
        self.heading.place(x=500,y=0)

        main_menu=Menu(master)
        self.master.config(menu=main_menu)

        fileMenu=Menu(main_menu)
        main_menu.add_cascade(label="File",menu=fileMenu)
        fileMenu.add_command(label="Home", command=self.Home)
        fileMenu.add_command(label="Exit", command=self.master.destroy)
        
        #lables  for the Non-database
        self.heading=Label(master,text="NON-DATABASE",font=('arial 25 bold italic'),fg='teal')
        self.heading.place(x=200,y=70)
         
        self.name_l=Label(master,text="Source_file_path",font=('arial 18 bold'))
        self.name_l.place(x=0,y=140)
        
        Button(master,text='Browse',font=('arial 18 bold italic'),fg='violet',command=self.open_file).place(x=620,y=130)

        self.heading=Label(master,text="DATABASE",font=('arial 25 bold italic'),fg='teal')
        self.heading.place(x=200,y=210)
 
        self.cp_l = Label(master, text="Host ", font=('arial 18 bold'))
        self.cp_l.place(x=0, y=280)
         
        self.cp_l = Label(master, text="User_name ", font=('arial 18 bold'))
        self.cp_l.place(x=0, y=330)
         
        self.cp_l = Label(master, text="Database_name ", font=('arial 18 bold'))
        self.cp_l.place(x=0, y=380)
         
        self.cp_l = Label(master, text="Password ", font=('arial 18 bold'))
        self.cp_l.place(x=0, y=430)
         
        self.cp_l = Label(master, text="Table_name ", font=('arial 18 bold'))
        self.cp_l.place(x=0, y=480)
         
 
        #enteries for extraction
        
        self.source_path_etry = Entry(master, width=25, font=('arial 18 bold'))
        self.source_path_etry.place(x=280, y=140)
 
        self.db_host_etry = Entry(master, width=25, font=('arial 18 bold'))
        self.db_host_etry.place(x=280, y=280)
         
        self.db_user_etry = Entry(master, width=25, font=('arial 18 bold'))
        self.db_user_etry.place(x=280, y=330)
         
        self.db_database_etry = Entry(master, width=25, font=('arial 18 bold'))
        self.db_database_etry.place(x=280, y=380)
         
        self.db_password_etry = Entry(master, width=25, font=('arial 18 bold'),show='*')
        self.db_password_etry.place(x=280, y=430)
         
        self.db_table_etry = Entry(master, width=25, font=('arial 18 bold'))
        self.db_table_etry.place(x=280, y=480)
 
         
        #text box for the log
        self.tbBox=Text(master,width=60,height=28)
        self.tbBox.place(x=750,y=80)
        self.tbBox.insert(END,"\n\nPrevious Credentials of database\n\t")
        self.tbBox.insert(END,"\n\tHost\t\t"+db_host)
        self.tbBox.insert(END,"\n\tDatabase\t\t"+db_database)
        self.tbBox.insert(END,"\n\tUser\t\t"+db_username)
        self.tbBox.insert(END,"\n\tTable\t\t"+table1)
        #self.tbBox.insert(END,"\n\n\tEnter '.db' in path for importing data from database")

 
         #button to add to import
        self.btn_add=Button(master,text='Import',width=22,height=2,bg='aqua',fg='black',font=('arial 10 bold'),command=self.import_data)
        self.btn_add.place(x=340,y=540)
 
        self.master.bind('&lt;Return&gt;', self.import_data)
    
    def Home(self):
        for widget in self.master.winfo_children():
            widget.destroy()
        self.master=Homepage(self.master)
    
    def open_file(self):
        file=filedialog.askopenfile(mode='r')
        if file:
            filepath = os.path.abspath(file.name)
            self.source_path_etry.insert(0,filepath)

    def exptotrans(self):
        for widget in self.master.winfo_children():
            widget.destroy()
        self.master=gotoTransformatin(self.master)
     

 
    def import_data(self, *args, **kwargs):
        # get from entries
        path = self.source_path_etry.get()
        #print(source_path)
       
        db_host1 = self.db_host_etry.get()
        db_user1 = self.db_user_etry.get()
        db_database1 = self.db_database_etry.get()
        db_password1 = self.db_password_etry.get()
        db_table1 = self.db_table_etry.get()
       
       
        # writing into xml file
       
        doc = minidom.Document()

        DB = doc.createElement('DB')
        doc.appendChild(DB)

        DBbody = doc.createElement('DBbody')
        DB.appendChild(DBbody)

        File_path = doc.createElement('file_path')
        DBbody.appendChild(File_path)
        File_path.appendChild(doc.createTextNode(path))

        if db_host1!='':
            global db_host
            db_host=db_host1
       
        host = doc.createElement('host')
        DBbody.appendChild(host)
        host.appendChild(doc.createTextNode(db_host))

        if db_database1!='':
            global db_database
            db_database=db_database1
        database = doc.createElement('db_database')
        DBbody.appendChild(database)
        database.appendChild(doc.createTextNode(db_database))

        if db_user1!='':
            global db_username
            db_username=db_user1
        user = doc.createElement('db_user')
        DBbody.appendChild(user)
        user.appendChild(doc.createTextNode(db_username))

        if db_password1!='':
            global db_pwd 
            db_pwd=db_password1
        passwd = doc.createElement('password')
        DBbody.appendChild(passwd)
        passwd.appendChild(doc.createTextNode(db_pwd))

        if db_table1!='':
            global table1
            table1=db_table1
        ntable1 = doc.createElement('db_table1')
        DBbody.appendChild(ntable1)
        ntable1.appendChild(doc.createTextNode(table1))

        #db_table2 = doc.createElement('db_table2')
        #DBbody.appendChild(db_table2)
        #db_table2.appendChild(doc.createTextNode(''))

        xml_str = doc.toprettyxml(indent = '\t')
        save_path_file = "dbandpath.xml"
        with open(save_path_file, "w") as f:
            f.write(xml_str)
        # print(table1)
        #Reading form xml file
        dbdoc = ET.parse('dbandpath.xml')
        for d in dbdoc.iterfind('DBbody'):
            db_username = d.findtext('db_user')
            db_pwd = d.findtext('password')
            db_host = d.findtext('host')
            db_database = d.findtext('db_database')
            path = d.findtext('file_path')
            table1 = d.findtext('db_table1')
        
        #storing the data in pandas dataframe
        global df_g
        global ext
        # df_g = Ext.get_input(path)
        for ch in path:
            if ch=='.':
                ext = path[path.index(ch)+1:] 
                #print(ext)
    
        if ext == 'csv':
            try:
                df_g = pd.read_csv(path)
                #print(df_g)
                self.tbBox.delete('1.0', END)
                if df_g.shape==(0,0):
                    self.tbBox.insert(END,'\nNo Records Found in the File')
                else:
                    self.tbBox.insert(END,"\n\ncsv file read successfully\n ")
                    self.tbBox.insert(END,'\n'+str(df_g.shape[0])+' records found')
                    self.tbBox.insert(END,'\n'+str(df_g.shape[1])+' attributes found\n')
                    self.source_path_etry.delete(0,END)
                    print_df_info(self,df_g)
            except:
                self.tbBox.insert(END,'Error while reading csv file ')
        
        elif ext == 'tsv':
                try:
                    df_g = pd.read_csv(path,sep='\t')
                    self.tbBox.delete('1.0', END)
                    if df_g.shape==(0,0):
                        self.tbBox.insert(END,'\nNo Records Found in the File')
                    else:
                        self.tbBox.insert(END,'tsv file read sucessfully ')
                        self.tbBox.insert(END,'\n'+str(df_g.shape[0])+' records found')
                        self.tbBox.insert(END,'\n'+str(df_g.shape[1])+' attributes found\n')
                        self.source_path_etry.insert(0,'')
                        print_df_info(self,df_g)
                    
                except:
                    self.tbBox.insert(END,'Error while reading tsv file ')

        elif ext == 'xlsx':
                try:
                    df_g = pd.read_excel(path)
                    self.tbBox.delete('1.0', END)
                    if df_g.shape==(0,0):
                        self.tbBox.insert(END,'\nNo Records Found in the File')
                    else:
                        self.tbBox.insert(END,"Excel file read sucessfully")
                        self.tbBox.insert(END,'\n'+str(df_g.shape[0])+' records found')
                        self.tbBox.insert(END,'\n'+str(df_g.shape[1])+' attributes found\n')
                        self.source_path_etry.insert(0,'')
                        print_df_info(self,df_g)
                except:
                    self.tbBox.insert(END,'Error while reading Excel file ')
                        
        elif ext == 'json':
                try:
                    df_g = pd.read_json(path,encoding='utf-8-sig')
                    self.tbBox.delete('1.0', END)
                    if df_g.shape==(0,0):
                        self.tbBox.insert(END,'\nNo Records Found in the File')
                    else:
                        self.tbBox.insert(END,"json file read sucessfully")
                        self.tbBox.insert(END,'\n'+str(df_g.shape[0])+' records found')
                        self.tbBox.insert(END,'\n'+str(df_g.shape[1])+' attributes found\n')
                        self.source_path_etry.insert(0,'')
                        print_df_info(self,df_g)
                except:
                    self.tbBox.insert(END,'Error while reading json file ') 
        # Function needs to be changed for db             
        elif ext == 'txt':
            try:
                df_g = np.loadtxt(path, delimiter=',', skiprows=1, dtype=str)
                self.tbBox.delete('1.0', END)
                if df_g.shape==(0,0):
                    self.tbBox.insert(END,'\nNo Records Found in the File')
                else:
                    self.tbBox.insert(END,"txt file read sucessfully")
                    self.tbBox.insert(END,'\n'+str(df_g.shape[0])+' records found')
                    self.tbBox.insert(END,'\n'+str(df_g.shape[1])+' attributes found\n')
                    self.source_path_etry.insert(0,'')
                    print_df_info(self,df_g)
            except:
                self.tbBox.insert(END,'Error while reading text file ') 
      
        elif path=='':
            yesno = mb.askquestion("Warning","Proceed With Database?")
            if yesno == 'yes':
                try:
                    db_connection_str = 'mysql+pymysql://{}:{}@{}/{}'.format(db_username,db_pwd,db_host,db_database)
                    db_connection = create_engine(db_connection_str,echo=False)
                    df_g = pd.read_sql('SELECT * FROM {}'.format(table1), con=db_connection)
                    if df_g.shape == (0,0):
                        self.tbBox.insert(END,"\n\nNo records found")
                    else:
                        self.tbBox.delete('1.0', END)
                        self.tbBox.insert(END,"Data has been imported sucessfully")
                        self.tbBox.insert(END,'\n'+str(df_g.shape[0])+' records found')
                        self.tbBox.insert(END,'\n'+str(df_g.shape[1])+' attributes found\n')
                        print_df_info(self,df_g)
                except:
                        self.tbBox.insert(END,'Error importing data from database')
            
        Button(self.master,text='TRANSFORM==>',width=16,height=2,bg='violet',fg='white',font=('arial 18 bold'),command=self.exptotrans).place(x=1080,y=540)
            
            
class Merge:
    def __init__(self,master,*args,**kwargs):
        self.master=master
        self.heading=Label(master,text="Merge",font=('arial 35 bold italic'),fg='teal')
        self.heading.place(x=600,y=0)

        #main_menu=Menu(master)
        #self.master.config(menu=main_menu)

        #fileMenu=Menu(main_menu)
        #main_menu.add_cascade(label="File",menu=fileMenu)
        #fileMenu.add_command(label="Home", command=self.Home)
        #fileMenu.add_command(label="Exit", command=self.master.destroy)
        
        #lables  for the Non-database
        self.heading=Label(master,text="NON-DATABASE",font=('arial 25 bold italic'),fg='teal')
        self.heading.place(x=200,y=70)
         
        self.name_l=Label(master,text="Source_file_path",font=('arial 18 bold'))
        self.name_l.place(x=0,y=140)
        
        Button(master,text='Browse',font=('arial 18 bold italic'),fg='violet',command=self.open_file).place(x=620,y=130)

        self.heading=Label(master,text="DATABASE",font=('arial 25 bold italic'),fg='teal')
        self.heading.place(x=200,y=210)
 
        self.cp_l = Label(master, text="Host ", font=('arial 18 bold'))
        self.cp_l.place(x=0, y=280)
         
        self.cp_l = Label(master, text="User_name ", font=('arial 18 bold'))
        self.cp_l.place(x=0, y=330)
         
        self.cp_l = Label(master, text="Database_name ", font=('arial 18 bold'))
        self.cp_l.place(x=0, y=380)
         
        self.cp_l = Label(master, text="Password ", font=('arial 18 bold'))
        self.cp_l.place(x=0, y=430)
         
        self.cp_l = Label(master, text="Table_name ", font=('arial 18 bold'))
        self.cp_l.place(x=0, y=480)
         
 
        #enteries for extraction
        
        self.source_path_etry = Entry(master, width=25, font=('arial 18 bold'))
        self.source_path_etry.place(x=280, y=140)
 
        self.db_host_etry = Entry(master, width=25, font=('arial 18 bold'))
        self.db_host_etry.place(x=280, y=280)
         
        self.db_user_etry = Entry(master, width=25, font=('arial 18 bold'))
        self.db_user_etry.place(x=280, y=330)
         
        self.db_database_etry = Entry(master, width=25, font=('arial 18 bold'))
        self.db_database_etry.place(x=280, y=380)
         
        self.db_password_etry = Entry(master, width=25, font=('arial 18 bold'),show='*')
        self.db_password_etry.place(x=280, y=430)
         
        self.db_table_etry = Entry(master, width=25, font=('arial 18 bold'))
        self.db_table_etry.place(x=280, y=480)
 
         
        #text box for the log
        self.tbBox=Text(master,width=60,height=28)
        self.tbBox.place(x=750,y=80)
        self.tbBox.insert(END,"\n\nPrevious Credentials of database\n\t")
        self.tbBox.insert(END,"\n\tHost\t\t"+db_host)
        self.tbBox.insert(END,"\n\tDatabase\t\t"+db_database)
        self.tbBox.insert(END,"\n\tUser\t\t"+db_username)
        self.tbBox.insert(END,"\n\tTable\t\t"+table1)
        self.tbBox.insert(END,"\n\n\tEnter '.db' in path for importing data from database")

 
        #button to add to import
        self.btn_add=Button(master,text='Import',width=22,height=2,bg='aqua',fg='black',font=('arial 10 bold'),command=self.import_data)
        self.btn_add.place(x=340,y=540)
 
        self.master.bind('&lt;Return&gt;', self.import_data)

    def open_file(self):
        file=filedialog.askopenfile(mode='r')
        if file:
            filepath = os.path.abspath(file.name)
            self.source_path_etry.insert(0,filepath)

    def import_data(self, *args, **kwargs):
        # get from entries
        path = self.source_path_etry.get()
        #print(source_path)
        db_host1 = self.db_host_etry.get()
        db_user1 = self.db_user_etry.get()
        db_database1 = self.db_database_etry.get()
        db_password1 = self.db_password_etry.get()
        db_table1 = self.db_table_etry.get()
        
        
        # writing into xml file
    
        doc = minidom.Document()

        DB = doc.createElement('DB')
        doc.appendChild(DB)

        DBbody = doc.createElement('DBbody')
        DB.appendChild(DBbody)

        File_path = doc.createElement('file_path')
        DBbody.appendChild(File_path)
        File_path.appendChild(doc.createTextNode(path))

        if db_host1!='':
            global db_host
            db_host=db_host1
        
        host = doc.createElement('host')
        DBbody.appendChild(host)
        host.appendChild(doc.createTextNode(db_host))

        if db_database1!='':
            global db_database
            db_database=db_database1
        database = doc.createElement('db_database')
        DBbody.appendChild(database)
        database.appendChild(doc.createTextNode(db_database))

        if db_user1!='':
            global db_username
            db_username=db_user1
        user = doc.createElement('db_user')
        DBbody.appendChild(user)
        user.appendChild(doc.createTextNode(db_username))

        if db_password1!='':
            global db_pwd 
            db_pwd=db_password1
        passwd = doc.createElement('password')
        DBbody.appendChild(passwd)
        passwd.appendChild(doc.createTextNode(db_pwd))

        if db_table1!='':
            global table1
            table1=db_table1
        ntable1 = doc.createElement('db_table1')
        DBbody.appendChild(ntable1)
        ntable1.appendChild(doc.createTextNode(table1))

            #db_table2 = doc.createElement('db_table2')
            #DBbody.appendChild(db_table2)
            #db_table2.appendChild(doc.createTextNode(''))

        xml_str = doc.toprettyxml(indent = '\t')
        save_path_file = "dbandpath.xml"
        with open(save_path_file, "w") as f:
            f.write(xml_str)
            # print(table1)
            #Reading form xml file
        dbdoc = ET.parse('dbandpath.xml')
        for d in dbdoc.iterfind('DBbody'):
            db_username = d.findtext('db_user')
            db_pwd = d.findtext('password')
            db_host = d.findtext('host')
            db_database = d.findtext('db_database')
            path = d.findtext('file_path')
            table1 = d.findtext('db_table1')
            
            #storing the data in pandas dataframe
        global df_g2
        global ext
        # df_g = Ext.get_input(path)
        for ch in path:
            if ch=='.':
                ext = path[path.index(ch)+1:] 
                    #print(ext)
        
        if ext == 'csv':
            try:
                df_g2 = pd.read_csv(path)
                #print(df_g)
                self.tbBox.delete('1.0', END)
                #self.tbBox.insert(END,df_g)
                self.tbBox.insert(END,"\n\ncsv file read successfully\n ")
                self.tbBox.insert(END,'\n'+str(df_g2.shape[0])+' records found')
                self.tbBox.insert(END,'\n'+str(df_g2.shape[1])+' attributes found\n')
                self.source_path_etry.delete(0,END)
            except:
                self.tbBox.insert(END,'Error while reading csv file ')
        
        elif ext == 'tsv':
            try:
                df_g2 = pd.read_csv(path,sep='\t')
                self.tbBox.delete('1.0', END)
                self.tbBox.insert(END,'tsv file read sucessfully ')
                self.tbBox.insert(END,'\n'+str(df_g2.shape[0])+' records found')
                self.tbBox.insert(END,'\n'+str(df_g2.shape[1])+' attributes found\n')
                self.source_path_etry.insert(0,'')
                        
            except:
                self.tbBox.insert(END,'Error while reading tsv file ')

        elif ext == 'xlsx':
            try:
                df_g2 = pd.read_excel(path)
                self.tbBox.delete('1.0', END)
                self.tbBox.insert(END,"Excel file read sucessfully")
                self.tbBox.insert(END,'\n'+str(df_g2.shape[0])+' records found')
                self.tbBox.insert(END,'\n'+str(df_g2.shape[1])+' attributes found\n')
                self.source_path_etry.insert(0,'')
            except:
                self.tbBox.insert(END,'Error while reading Excel file ')
                            
        elif ext == 'json':
            try:
                df_g2 = pd.read_json(path,encoding='utf-8-sig')
                self.tbBox.delete('1.0', END)
                self.tbBox.insert(END,"json file read sucessfully")
                self.tbBox.insert(END,'\n'+str(df_g2.shape[0])+' records found')
                self.tbBox.insert(END,'\n'+str(df_g2.shape[1])+' attributes found\n')
                self.source_path_etry.insert(0,'')
            except:
                self.tbBox.insert(END,'Error while reading json file ') 
            # Function needs to be changed for db             
        elif ext == 'txt':
            try:
                df_g2 = np.loadtxt(path, delimiter=',', skiprows=1, dtype=str)
                self.tbBox.delete('1.0', END)
                self.tbBox.insert(END,"txt file read sucessfully")
                self.tbBox.insert(END,'\n'+str(df_g2.shape[0])+' records found')
                self.tbBox.insert(END,'\n'+str(df_g2.shape[1])+' attributes found\n')
                self.source_path_etry.insert(0,'')
            except:
                self.tbBox.insert(END,'Error while reading text file ') 
        
        elif ext == 'db' or ext == 'sql' or path=='':
            try:
                        
                db_connection_str = 'mysql+pymysql://{}:{}@{}/{}'.format(db_username,db_pwd,db_host,db_database)
                db_connection = create_engine(db_connection_str,echo=False)
                df_g2 = pd.read_sql('SELECT * FROM {}'.format(table1), con=db_connection)

                self.tbBox.delete('1.0', END)
                self.tbBox.insert(END,"Data has been imported sucessfully")
                self.tbBox.insert(END,'\n'+str(df_g.shape[0])+' records found')
                self.tbBox.insert(END,'\n'+str(df_g.shape[1])+' attributes found\n')
            except:
                self.tbBox.insert(END,'Error importing data from database')

        Button(self.master,text='MERGE==>',width=16,height=2,bg='violet',fg='white',font=('arial 18 bold'),command=lambda:self.activatedf_g2(df_g2)).place(x=1080,y=640)

    def activatedf_g2(self,df):
        global df_g
        global df_g2
        df_g2 = df
        for widget in self.master.winfo_children():
            widget.destroy()
        self.master=gotoTransformatin(self.master,1)       
                       
                       
class transform:
    def __init__(self, master, *args, **kwargs):
        global df_g 
        self.master=master
        self.tbBox=Text(master,width=60,height=28)
        self.tbBox.place(x=800,y=80)
        self.tbBox.insert(END,"1.click on BASIC\n\t to replace NAN values and drop duplicates\n2.cilck on TRANPOSE\n\t to interchange rows and columns\n3.click on TIME CONVERSION\n\t to convert record in a date time standard\n4.click on VALIDATION\n\t to remove the row if the first column is NULL\n5. click on FILL NAN\n\t to fill the nan values\n6. click on ENCODE\n\t to do encoding of the data\n7. click on MERGE\n\t to merge the two dataframes\n8. click on STANDARDIZATION\n\t to standardize the data")
         
         
        main_menu=Menu(master)
        self.master.config(menu=main_menu)

        fileMenu=Menu(main_menu)
        main_menu.add_cascade(label="File",menu=fileMenu)
        fileMenu.add_command(label="Home", command=self.Home)
        fileMenu.add_command(label="Exit", command=self.master.destroy)

         
        self.master = master
        self.heading = Label(master, text="Transforming the Data", font=('arial 35 bold italic'), fg='teal')
        self.heading.place(x=400, y=0)

        self.basic1 = Label(master, text="Basic ", font=('arial 18 bold'))
        self.basic1.place(x=0, y=90)

        self.transpose = Label(master, text="Transpose", font=('arial 18 bold'))
        self.transpose.place(x=0, y=140)

        self.time = Label(master, text="Time Conversion", font=('arial 18 bold'))
        self.time.place(x=0, y=190)

        self.validation = Label(master, text="Validation", font=('arial 18 bold'))
        self.validation.place(x=0, y=240)

        self.fill = Label(master, text="Fill NAN Values", font=('arial 18 bold'))
        self.fill.place(x=0, y=290)

        self.label_encod = Label(master, text="Label Encoding", font=('arial 18 bold'))
        self.label_encod.place(x=0, y=340)

        self.onehot_encode = Label(master, text="OneHot Encoding", font=('arial 18 bold'))
        self.onehot_encode.place(x=0, y=390)

        self.merge1 = Label(master, text="Merge", font=('arial 18 bold'))
        self.merge1.place(x=0, y=440)

        self.stand = Label(master, text="Standardization", font=('arial 18 bold'))
        self.stand.place(x=0, y=490)
         
        self.entry_time = Entry(master, width=25, font=('arial 18 bold'))
        self.entry_time.place(x=200, y=190)
        
        self.entry_valid = Entry(master, width=25, font=('arial 18 bold'))
        self.entry_valid.place(x=200, y=240)
         
        self.entry_fill_1 = Entry(master, width=12, font=('arial 18 bold'))
        self.entry_fill_1.place(x=200, y=290)

        self.entry_fill_2 = Entry(master, width=12, font=('arial 18 bold'))
        self.entry_fill_2.place(x=368, y=290)
         
        self.entry_label_encode = Entry(master, width=25, font=('arial 18 bold'))
        self.entry_label_encode.place(x=200, y=340)

        self.entry_onehot_encode = Entry(master, width=25, font=('arial 18 bold'))
        self.entry_onehot_encode.place(x=210, y=390)
         
        self.entry_merge = Entry(master, width=25, font=('arial 18 bold'))
        self.entry_merge.place(x=200, y=440)
         
        self.entry_standard = Entry(master, width=25, font=('arial 18 bold'))
        self.entry_standard.place(x=200, y=490)
        #button for basic function
        self.btn_basic=Button(master,text='OPERATE',width=20,height=2,bg='steelblue',fg='white',command=lambda:self.basic(df_g))
        self.btn_basic.place(x=550,y=90)
        #button for transpose
        self.btn_transpose=Button(master,text='OPERATE',width=20,height=2,bg='steelblue',fg='white',command=lambda:self.transform_pose(df_g))
        self.btn_transpose.place(x=550,y=140)
        #button for time stamp
        self.btn_time=Button(master,text='OPERATE',width=20,height=2,bg='steelblue',fg='white',command=lambda:self.convert_time_to_std(df_g))
        self.btn_time.place(x=550,y=190)
        #button for validation
        self.btn_valid=Button(master,text='OPERATE',width=20,height=2,bg='steelblue',fg='white',command=lambda:self.valid(df_g))
        self.btn_valid.place(x=550,y=240)
        #button for fill na
        self.btn_fill_1=Button(master,text='OPERATE',width=20,height=2,bg='steelblue',fg='white',command=lambda:self.fillx(df_g))
        self.btn_fill_1.place(x=550,y=290)
        #button for label_encoding
        self.btn_label_encode=Button(master,text='OPERATE',width=20,height=2,bg='steelblue',fg='white',command=lambda:self.label_encode(df_g))
        self.btn_label_encode.place(x=550,y=340)
        #button for onehot_encoding
        self.btn_onehot_encode=Button(master,text='OPERATE',width=20,height=2,bg='steelblue',fg='white',command=lambda:self.one_hotencode(df_g))
        self.btn_onehot_encode.place(x=550,y=390)
        #button for merge function
        self.btn_merge=Button(master,text='OPERATE',width=20,height=2,bg='steelblue',fg='white',command=self.merge)
        self.btn_merge.place(x=550,y=440)
        #button for data standardization
        self.btn_stand=Button(master,text='OPERATE',width=20,height=2,bg='steelblue',fg='white',command=NONE)
        self.btn_stand.place(x=550,y=490)

        self.btn_add=Button(master,text='Done',width=22,height=2,bg='aqua',fg='black',font=('arial 10 bold'),command=self.transtoload)
        self.btn_add.place(x=340,y=640)
    
    

    def Home(self):
        for widget in self.master.winfo_children():
            widget.destroy()
        self.master=Homepage(self.master)   

    def basic(self,df):
        self.tbBox.delete("1.0",END)
        self.tbBox.insert(END,"Performing Basic Operation!\n")
        self.tbBox.insert(END,"{} Null values found\n".format(df.isnull().sum()))
        
        df.replace(np.NAN, -1, inplace=True)
        features = df.columns
        for i in features:
            if df[i].dtypes == "object":
                self.tbBox.insert(END,"{} ".format(df[i].name))
        self.tbBox.insert(END,"\nThese are the catogorical columns\n")
        for i in features:
            if df[i].dtypes != "object":
                df.drop_duplicates(i, inplace=True)
        self.tbBox.insert(END,"Null and Duplicate Values Processed successfully!")
        #print_df_info(self,df_g)

    def transform_pose(self,df):
        self.tbBox.delete("1.0",END)
        self.tbBox.insert(END,"Performing Transpose Operation!\n")
        global df_g
        df_g = df.T
        self.tbBox.insert(END,"Transpose Performed Successfully!")
        print_df_info(self,df_g)

    def valid(self,df):
        self.entry_valid.delete(0,END)
        self.tbBox.delete("1.0",END)
        global df_g
        feature = df.columns
        df.replace(np.NAN, -1, inplace=True)
        a = []
        for i in range(len(feature) - 1):
            a.append(df.columns[i])
        b = a[0:2]
        self.tbBox.delete("1.0",END)
        for i in range(len(b)):
            df = df.drop(index=df[df[[a[i]][0]] == -1.0].index)
            self.tbBox.insert(END,"Validation Done")
        df_g=df
        print_df_info(self,df_g)

    def convert_time_to_std(self,df):
        
        self.tbBox.delete("1.0",END)
        global df_g
        feature = []
        temptime = []
        
        col_name = self.entry_time.get()
        for i in df[col_name]:
            feature.append(i)
        for ch in feature:
            try:
                temptime.append(parse(ch))
            except:
                self.tbBox.insert(END,'Unable to convert the string into std time format')
        df_g[col_name]=temptime
        print_df_info(self,df_g)
        self.entry_time.delete(0,END)
        

    def label_encode(self,df):
        
        global df_g
        feature = []
        le = preprocessing.LabelEncoder()
        for i in df.columns:
            if df[i].dtypes == "object":
                feature.append(i)
        self.tbBox.insert(END,feature)
        c = []
        b = [str(x) for x in self.entry_onehot_encode.get().split()]
        for i in df.columns:
            if i not in b:
                c.append(i)
        for i in c:
            df[i] = le.fit_transform(df[i])
            self.tbBox.insert(END,"the label encoding is done...")
            self.tbBox.insert(END,df[i].unique())
        df_g=df
        print_df_info(self,df_g)
        self.entry_label_encode.delete(0,END)

    def one_hotencode(self,df):
        global df_g
        feature = []
        for i in df.columns:
            if df[i].dtypes == "object":
                feature.append(i)
        self.tbBox.insert(END,"{} ".format(feature))
        c =[]
        b = [str(x) for x in self.entry_onehot_encode.get().split()]
        for i in df.columns:
            if i not in b:
                c.append(i)
        for i in c:
            df = pd.get_dummies(df, columns=[i])
            #df.drop(df[i], axis=0)
        df_g=df
        print_df_info(self,df_g)
        self.entry_onehot_encode.delete(0,END)

    def fillx(self,df):
        
        global df_g
        b = []
        for i in df.columns:
            if df[i].dtype == 'object':
                b.append(i)
        self.tbBox.insert(END,"These are the columns which are in the object datatype kindly change it before moving further : {}".format(b))
        d = []
        c = [str(x) for x in self.entry_fill_1.get().split()]
        for i in df.columns:
            if i not in c:
                d.append(i)
        
        e = self.entry_fill_2.get()
        if e == "mean":
            for i in d:
                df[i].fillna(df[i].mean(), inplace=True)
            self.tbBox.insert(END,"{}".format(df.isnull().sum()))
        elif e == "median":
            for i in d:
                df[i].fillna(df[i].median(), inplace=True)
            self.tbBox.insert(END,"{}".format(df.isnull().sum()))
        elif e == "mode":
            for i in d:
                df[i].fillna(df[i].mode(), inplace=True)
            self.tbBox.insert(END,"{}".format(df.isnull().sum()))
        elif e=='':
            mb.showinfo('Warning', 'No Value Passed')
        else:
            for i in d:
                df[i].fillna(values=e,inplace=True)
            self.tbBox.insert(END,"{}".format(df.isnull().sum()))
        df_g=df
        print_df_info(self,df_g)
        self.entry_fill_1.delete(0,END)
        self.entry_fill_2.delete(0,END)
        
        
    def mergee(self,df_g,df_g2):
        df_g.info()
        df_g2.info()
        c = []
        b = []
        d = []
        for i in df_g.columns:
            if i in df_g2.columns:
                d.append(i)
        if len(d)>0:
            #self.tbBox.insert(END,"\nThe matching columns are:{} ".format(d))
            res = mb.askquestion("Confirmation","The matching columns are:{} \nDo You Want to Merge These Tables Based on these columns".format(d))
        if res == "yes":
            result = pd.concat([df_g, df_g2])
            self.tbBox.insert(END,"Tables Merged Successfully!")
            print_df_info(self,df_g)
        else:
            self.tbBox.insert(END,"Cannot Merge Tables!")
        self.entry_merge.delete(0,END)
        

    def merge(self):
        for widget in self.master.winfo_children():
            widget.destroy()
        self.master=gotomerge(self.master)
        
        
        #global df_g2
        #df_g2.info()
        

    def standard(self,df):
        global df_g
        self.tbBox.insert(END,df.columns)
        b = []
        for i in df.columns:
            if df[i].dtype == 'object':
                b.append(i)
        #self.tbBox.insert(END,"These are the columns which are in the object datatype kindly change it before moving further : {}".format(b))
        #self.tbBox.insert(END,"1. Enter change to change the value\n 2. Enter YES to standardinze the data\n 3. Enter NO to exit")
        d = []
        c = [str(x) for x in self.entry_standard.get().split()]
        for i in df.columns:
            if i not in c:
                d.append(i)
        try:
            for i in d:
                df[i] = df[i] / df[i].abs().max()
        except:
            self.tbBox.insert(END,'Can not perform standardization!Please perform encoding to continue!')
        self.tbBox.insert(END,"{}".format(df.head(5)))
        df_g=df
        print_df_info(self,df_g)
        self.entry_standard.delete(0,END)

    def transtoload(self):
        for widget in self.master.winfo_children():
            widget.destroy()
        self.master=gotoLoad(self.master)

              
class Load:
    def __init__(self,master,*args,**kwargs):
        self.master=master
        self.heading=Label(master,text="LOAD INTO SQL DATABASE",font=('arial 35 bold italic'),fg='teal')
        self.heading.place(x=400,y=0)
        
        main_menu=Menu(master)
        self.master.config(menu=main_menu)

        fileMenu=Menu(main_menu)
        main_menu.add_cascade(label="File",menu=fileMenu)
        fileMenu.add_command(label="Home", command=self.Home)
        fileMenu.add_command(label="Exit", command=self.master.destroy)

        self.host_l = Label(master, text="Host ", font=('arial 18 bold'))
        self.host_l.place(x=0, y=90)
        
        self.port_l = Label(master, text="Port ", font=('arial 18 bold'))
        self.port_l.place(x=0, y=140)
        
        self.user_l = Label(master, text="User_name ", font=('arial 18 bold'))
        self.user_l.place(x=0, y=190)
        
        self.database_l = Label(master, text="Database_name ", font=('arial 18 bold'))
        self.database_l.place(x=0, y=240)
        
        self.pwd_l = Label(master, text="Password ", font=('arial 18 bold'))
        self.pwd_l.place(x=0, y=290)
        
        self.table_l = Label(master, text="Table_name ", font=('arial 18 bold'))
        self.table_l.place(x=0, y=340)

        #enteries for extraction

        self.host_etry = Entry(master, width=25, font=('arial 18 bold'))
        self.host_etry.place(x=280, y=90)
        
        self.port_etry = Entry(master, width=25, font=('arial 18 bold'))
        self.port_etry.place(x=280, y=140)
        
        self.user_etry = Entry(master, width=25, font=('arial 18 bold'))
        self.user_etry.place(x=280, y=190)
        
        self.database_etry = Entry(master, width=25, font=('arial 18 bold'))
        self.database_etry.place(x=280, y=240)
        
        self.password_etry = Entry(master, width=25, font=('arial 18 bold'),show='*')
        self.password_etry.place(x=280, y=290)
        
        self.table_etry = Entry(master, width=25, font=('arial 18 bold'))
        self.table_etry.place(x=280, y=340)

        #self.cp_e = Entry(master, width=25, font=('arial 18 bold'))
        #self.cp_e.place(x=280, y=170)
        
        #text box for the log
        self.tbBox=Text(master,width=60,height=28)
        self.tbBox.place(x=750,y=80)
        self.tbBox.insert(END,"\n\nPrevious Credentials of database\n\t")
        self.tbBox.insert(END,"\n\tHost\t\t"+db_host)
        self.tbBox.insert(END,"\n\tDatabase\t\t"+db_database)
        self.tbBox.insert(END,"\n\tUser\t\t"+db_username)
        self.tbBox.insert(END,"\n\tTable\t\t"+table1)
        #self.tbBox.insert(END,"\n\n\tEnter '.db' in path for importing data from database")


        #button to add to import
        self.btn_add=Button(master,text='Load',width=22,height=2,bg='aqua',fg='black',font=('arial 10 bold'),command=self.load_data)
        self.btn_add.place(x=340,y=540)

    def call(self):
        res = mb.askquestion('Warning',
                            'Table Already Exist! Do you want to append in the table')
        if res == 'yes' :
            return 1
        else:
            mb.showinfo('Suggestion', 'Please Change the Table Name!')
            return 0
            

    def Home(self):
        for widget in self.master.winfo_children():
            widget.destroy()
        self.master=Homepage(self.master)

    def load_data(self, *args, **kwargs):
        #global df
        global df_g
        #get from entries
        #source_path = self.source_path_etry.get()
        
        db_host = self.host_etry.get()
        db_port = self.port_etry.get()
        db_user = self.user_etry.get()
        db_password = self.password_etry.get()
        db_database = self.database_etry.get()
        db_table1 = self.table_etry.get()

        dbdoc = ET.parse('dbandpath.xml')
        for d in dbdoc.iterfind('DBbody'):
            if db_user=='':
                db_user = d.findtext('db_user')
            if db_password=='':
                db_password = d.findtext('password')
            if db_host == '':
                db_host = d.findtext('host')
            db_database = d.findtext('db_database')
            if db_table1 == '':
                db_table1 = d.findtext('db_table1')
        
        
        mydb = mysql.connector.connect(
                                        host=db_host,
                                        user=db_username,
                                        password=db_password,
                                        database=db_database)
        try:
            cur= mydb.cursor()
            cur.execute("SELECT * FROM "+db_table1)
            if self.call()==1:
                try:
                    engine = create_engine('mysql+pymysql://{}:{}@{}/{}'.format(db_user,db_password,db_host,db_database), echo = False)
                    df_g.to_sql(name = db_table1, con = engine, if_exists = 'append', index = False)
                    self.tbBox.insert(END,"\nTable already exists! Data appended to table")
                except:
                    mb.showinfo('Suggestion', 'Table Dimension Differs! \nPlease Change the Table Name!')
        except:
            engine = create_engine('mysql+pymysql://{}:{}@{}/{}'.format(db_user,db_password,db_host,db_database), echo = False)
            df_g.to_sql(name = db_table1, con = engine, if_exists = 'append', index = False)
            self.tbBox.insert(END,"\nTable Created and "+str(df_g.shape[0])+" Records Loaded!")
        
        
def gotoExtraction(master):    
    master.geometry("1366x768+0+0")
    master.title("ETL System")
    b=Extraction(master)
    master.mainloop()


def gotoTransformatin(master,mer=0): 
    #root=Tk()
    b=transform(master)
    master.geometry("1366x768+0+0")
    master.title("ETL System")
    master.mainloop()
    
def gotoLoad(master): 
    #root=Tk()
    b=Load(master)
    master.geometry("1366x768+0+0")
    master.title("ETL System")
    master.mainloop()
    
def gotoDashboard(): 
    #wb.open('dashboard_page')
    global df_g
    global html_str
    current_time = datetime.datetime.now() 
    row = str(df_g.shape[0])
    drow = str(df_g.shape[0])
    sync = str(current_time)[:-7]
    error = '0' 

    html_str = """<!DOCTYPE HTML>
    <html lang="en">
    <head>
    <style>
        body{background-color: #a9a99f;}
    </style>  
        <title>ETL dashboard</title>
        
        <a href="https://ibb.co/DC0X4Wr"><img src="https://i.ibb.co/Wym9n5V/rjas.jpg" alt="rjas"  width="300" height="300"></a>
    
    </head>
    <body>
    <div class="dashboard">
        <section class="navigation">
            <title>ETL dashboard</title>
        </section>
        <section class="main">

            </div>
            <div class="title">
            <h1 align="center">Extraction,Transform and Load</h1>
            <h2 align="center">DASHBOARD</h2>
            </div>
    <div class="whole">
            
                <div class="process_1">

                        <h2 align="center"><b>Total records in source</b></h2>
                        <p align="center">records """+row+"""</p>
                        
                    </div>
            </div>
    <div class="process_2">

                        <h2 align="center"><b>Total recods in destination</b></h2>
                        <p align="center">records """+drow+"""</p>
                    </div>
            </div>
    <div class="process_3">

                        <h2 align="center"><b>last sync data</b></h2>
                        <p align="center">sync data """+sync+"""</p>
                    </div>
            </div>
            <div class="process_4">

                        <h2 align="center"><b>Errors in syncing</b></h2>
                        <p align="center">error """+error+"""</p>
                    </div>
    <div class="process_5">
        <h2 align="center"><b>To fix the error</b> </h2>
            <!--<button align="center" type="button" onclick="alert('')">Fix</button>-->
            <button type="button" onclick="alert('Data Synced!')" style="margin-left:auto;margin-right:auto;display:block;margin-top:0%;margin-bottom:0%">
    sync
    </button>
            </div>
        </section>
    </div>
        
        <section class="secondary">
            
        </section>
    </div>
    </body>
    </html>"""

    #html_str = html_str % (row,drow,sync,error)
    f = open('HTML-3.html', 'w')
    f.write(html_str)
    webbrowser.open_new_tab(r"C:\Users\Administrator\VS Code\E-Mind\HTML-3.html")

def gotomerge(root):   
    #root=Tk() 
    root.geometry("1366x768+0+0")
    root.title("ETL System")
    return(Merge(root))

root=Tk()
b=Homepage(root)
root.geometry("1366x768+0+0")
#root.title("Inventary Control System")
root.mainloop()


