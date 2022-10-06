import os.path
import pandas as pd
import numpy as np
import cx_Oracle
import json,logging
import csv
from tqdm import tqdm
import os,shutil
from time import sleep
from datetime import datetime


class ExtractFromOracle:
    '''Utility Class providing functions to run a ata comparison between two databases'''
    logging.basicConfig(filename='Comparison_tool.log', format='%(filename)s:  %(asctime)s - %(message)s',level=logging.DEBUG)
    def __init__(self):
        try:
            self.connection = None 
            self.source_db=""
            self.target_db=""
        except (ValueError,FileNotFoundError) as e:
            print("Error in Initializing !! ",e)
      
    def createDirectory(self,user_isSourceOrTarget:str):
        logging.debug("Reading Json file for creating relevant Directories")
        with open(file="db_creds.json", mode='r') as cfile:
            data = json.load(cfile)
        if user_isSourceOrTarget=='S':
            src_hostname = data.get('Source_DB').get('hostname')
            source_db = str(src_hostname)+'_REF_'+datetime.now().strftime("%Y%m%d%H%M")
            print("Source DB Extracts will be written to :  ", source_db)
            if os.path.exists(source_db):
                shutil.rmtree(source_db)
            os.makedirs(source_db)
            logging.debug("Source Directory created ")
            self.source_db = source_db
        else:        
            tar_hostname = data.get('Target_DB').get('hostname')
            target_db = str(tar_hostname)+'_REF_'+datetime.now().strftime("%Y%m%d%H%M")        
            print("Target DB Extracts will be written to  :  ", target_db)
            if os.path.exists(target_db):
                shutil.rmtree(target_db)
            os.makedirs(target_db)
            logging.debug("Target Directory Created ")
            self.target_db = target_db

        
    def get_connection(self, sourceOrTargetDB):
        try:
            with open(file="db_creds.json", mode='r') as filehandle:
                if sourceOrTargetDB == 'S':
                    print("Connecting to Source Table")
                    data = json.load(filehandle)
                    creds = data.get('Source_DB')
                if sourceOrTargetDB == 'T':
                    print("Connecting to Target Table")
                    data = json.load(filehandle)
                    creds = data.get('Target_DB')
                # print(creds)
            #create Directories for extracts 
            self.createDirectory(user_isSourceOrTarget=sourceOrTargetDB)
            # if your DB has a SID put sid in service name variable else use service name
            dsnStr = cx_Oracle.makedsn(creds.get('hostname'), creds.get(
                'port'), service_name=creds.get('sid'))
            pool = cx_Oracle.SessionPool(
                creds.get('username'), creds.get('password'), dsn=dsnStr)
            self.connection_handle = pool.acquire()
            # show the version of the Oracle Database
            print("Oracle Version of DB is : ", self.connection_handle.version)
            return self.connection_handle            
        except (cx_Oracle.Error, ValueError, FileNotFoundError) as e:
            print(e)
            exit(-1)

    def createExtractOfTable(self, tablename: str, isSourceOrTarget: str):
        try:
            if isSourceOrTarget == 'S':
                f = open(os.path.join(self.source_db, f"{tablename}.CSV"), "w")
            else:
                f = open(os.path.join(self.target_db, f"{tablename}.CSV"), "w")

            writer = csv.writer(f, delimiter='|', lineterminator="\n")
            with self.connection_handle.cursor() as cursor:
                print(f"Extracting table {tablename}")
                sql = f"select * from {tablename}"
                cursor.execute(sql)
                allrows = cursor.fetchall()
                col_names = [row[0] for row in cursor.description]
                writer.writerow(col_names)
                for onerow in allrows:
                    writer.writerow(onerow)
        except (cx_Oracle.Error, csv.Error) as ce:
            print(ce)
        finally:
            f.close()
            if os.stat(f.name).st_size == 0:
                print("Removing Empty Files")
                os.remove(f.name)

    def runThroughFiles(self,src_dir :str,tar_dir :str):
        changesIdentified=0 #increment this count if and only if something is identified
        styling = '''
                <html lang="en">
                <html>
                <body>
                <meta charset="UTF-8">
                <style>
                hr.new1 {  border-top: 1.5px solid black;}
                /* Style the body */
                body {
                font-family: Arial;
                margin: 0;
                padding:0;
                }                
                .header {
                padding: 60px;
                text-align: center;
                background: #4572ba;
                color: white;
                font-size: 20px;
                } .content {
                padding:20px; 
                width: fit-content;
                background: #88B04B;}
                </style>'''

        outputReportName = "Comparison_Report_" + \
                           datetime.now().strftime("%Y%m%d%H%M%S") + ".html"
        sourcefiles = os.listdir(src_dir)
        targetfiles = os.listdir(tar_dir)
        print(f"Comparing Source Files present in {src_dir}")
        print(f"Comparing Target Files present in {tar_dir}")
        print("\n")
        results = open(outputReportName, "a")
        results.writelines(styling)
        results.writelines(
            "<div class='header'><h1> REFERENCE DATA COMPARISION REPORT </h1></div>")
        results.writelines("<div class='content'>")
        for i in tqdm(sourcefiles):
            sleep(0.1)
            if i in targetfiles:
                logging.debug(f"File {i} Present in target")
                d1 = pd.read_csv(os.path.join(src_dir, i),
                                 sep='|', encoding="windows-1252")
                d2 = pd.read_csv(os.path.join(tar_dir, i),
                                 sep='|', encoding="windows-1252")
                # d3 = pd.concat([d1, d2]).drop_duplicates(keep=False)

                #######################################################
                # 1) when rows and columns are equal report Data integrity only
                if ((d1.shape[0] == d2.shape[0]) and (d1.shape[1] == d2.shape[1]) and (d1.equals(d2) == False)):
                    logging.debug("Case 1 satisfied")
                    try:
                        ne_stacked = (d1 != d2).stack()
                        changed = ne_stacked[ne_stacked]
                        changed.index.names = ['Row Number is ', 'Column Name ']
                        difference_locations = np.where(d1 != d2)
                        changed_from = d1.values[difference_locations]
                        changed_to = d2.values[difference_locations]
                        d3 = pd.DataFrame({'Source Value is     ': changed_from,
                                       'Target Value is    ': changed_to}, index=changed.index).dropna()
                        final = d3.reset_index(level='Row Number is ')
                        final['Row Number is '] = final['Row Number is '] + 1
                        if d3.empty == False:
                            results.write(f"<hr class='new1'><h3>Findings for table {i.split('.')[0]}</h3>")
                            results.write(
                            "<p>Some Mismatched values were found in the following Rows </p>")
                            results.writelines(final.to_html())
                            changesIdentified+=1
                    except ValueError as e:
                        pass

                # 2) when row count matches but cols doesnt , report missing columns
                if ((d1.shape[0] == d2.shape[0]) == True) and ((d1.shape[1] == d2.shape[1]) == False):
                    logging.debug("Case 2 satisfied")
                    results.write(
                        f"<hr class='new1'><h3>Findings for table {i.split('.')[0]}</h3>")
                    if (d1.columns.difference(d2.columns).empty) == False:
                        results.write("<p>Some Columns are Missing in Target Database </p>")
                        results.write(
                            "<p>Missing Columns are :" + str(d1.columns.difference(d2.columns).values) + "</p>")
                        d1 = d1.drop(d1.columns.difference(d2.columns).values, axis=1)
                        changesIdentified+=1
                    # check for missing columns in Master Database
                    if (d2.columns.difference(d1.columns).empty) == False:
                        results.write("<p>Some Columns Are Not Present in Master Database</p>")
                        results.write("<p>They are As follows: </p>")
                        results.write("<p>Missing Columns in Master DB are :" + str(
                            d2.columns.difference(d1.columns).values) + "</p>")
                        d2 = d2.drop(d2.columns.difference(d1.columns).values, axis=1)
                        changesIdentified+=1
                    # drop missing columns and check for integrity of rows which are matching
                    # d1 = d1.drop(d1.columns.difference(d2.columns).values, axis=1)
                    if (d1.equals(d2) == False):
                        results.write(
                            "<p>Few Row values are also not matching, they are as below </p>")
                        ne_stacked = (d1 != d2).stack()
                        changed = ne_stacked[ne_stacked]
                        changed.index.names = [
                            'Row Number is ', 'Column Name ']
                        difference_locations = np.where(d1 != d2)
                        changed_from = d1.values[difference_locations]
                        changed_to = d2.values[difference_locations]
                        d3 = pd.DataFrame({'Source Value is     ': changed_from,
                                           'Target Value is    ': changed_to}, index=changed.index).dropna()
                        final = d3.reset_index(level='Row Number is ')
                        final['Row Number is '] = final['Row Number is '] + 1
                        results.writelines(final.to_html())

                # 3) when rows dont  but cols match ,report missing rows and check for integrity of common rows in two frames
                if (((d1.shape[0] == d2.shape[0]) == False) and ((d1.shape[1] == d2.shape[1]) == True)):
                    logging.debug("Case 3 satisfied")
                    results.write(f"<hr class='new1'><h3>Findings for table {i.split('.')[0]}</h3>")
                    # Rows Which are Present in Master But not in Target
                    not_present_in_target = d1.merge(d2, how='outer', indicator=True).loc[
                        lambda x: x['_merge'] == 'left_only']
                    if (not_present_in_target.shape[0] > 0):
                        results.write(
                            "<p>Target Database Lacks Following Rows. These Are availble in Master DB. Please Add them in Target to sync</p>")
                        results.write(not_present_in_target.to_html())
                        changesIdentified+=1
                    # Rows Which are not present in Master But present in Source
                    not_present_in_master = d1.merge(d2, how='outer', indicator=True).loc[
                        lambda x: x['_merge'] == 'right_only']
                    if (not_present_in_master.shape[0] > 0):
                        results.write(
                            "<p>Master Database Lacks Following Rows. These Are availble in Target DB. Please Add them in Master to sync both</p>")
                        results.write(not_present_in_master.to_html())
                        changesIdentified+=1

                # 4) when rows and cols both dont match , also validate data integrity
                if (((d1.shape[0] == d2.shape[0]) == False) and ((d1.shape[1] == d2.shape[1]) == False)):
                    logging.debug("Case 4 satisfied")
                    results.write(
                        f"<hr class='new1'><h3>Findings for table {i.split('.')[0]}</h3>")
                    # print missing cols
                    if (d1.columns.difference(d2.columns).empty) == False:
                        results.write("<p>Some Columns are Missing in Target Database </p>")
                        results.write("<p>Missing Columns are :" + str(d1.columns.difference(d2.columns).values) + "</p>")
                        changesIdentified+=1
                    # check for missing columns in Master Database
                    if (d2.columns.difference(d1.columns).empty) == False:
                        results.write("<p>Some Columns Are Not Present in Master Database</p>")
                        results.write("<p>They are As follows: </p>")
                        results.write("<p>Missing Columns in Master DB are :" + str(d2.columns.difference(d1.columns).values) + "</p>")
                        changesIdentified+=1    

                    # print missing rows
                    extra_in_target2 = d1.merge(
                        d2, how='outer', indicator=True).loc[lambda x: x['_merge'] == 'right_only']
                    results.write("<p> Found Missing Rows : " + "</p>")
                    results.writelines(extra_in_target2.to_html())
        if changesIdentified==0:
            results.write("<b> Nothing to Report !!!! " + "</b>")
        results.writelines("</div></body></html>")  # end all div tags
        results.close()
        print("\nComparison is Finished Report Is Created ")

    def driveMenuOptions(self):
        user_inp = 0
        while user_inp != 4:
            print("\n\n-----------Ref Data Comparison Tool ---------------")
            print('''
                1. To extract All Tables \n
                2. To extract Specific Table\n
                3. Extracts Available Just Compare.\n
                4. To Exit Out \n''')
            user_inp = int(input("Select One Option  ---> "))
            logging.debug(f"User Input is {user_inp}")
            print("\n")
            if user_inp is not None:
                if user_inp == 1:
                    user_isSourceOrTarget = input(
                        '''  Which DB tables you want to extract ? Enter 'S' for Source or 'T' for Target (without single qoutes obviosly) \n
                             |-->''')
                    print("Starting Extracts of ALL tables")
                    logging.debug(f"User Input is {user_isSourceOrTarget}")
                    if (user_isSourceOrTarget != 'S' and user_isSourceOrTarget != 'T'):
                        exit(-1)
                    print("Connecting To Relevant Database ...")
                    self.get_connection(sourceOrTargetDB=user_isSourceOrTarget)
                    with open("Table_List.txt", "r") as filehandle:
                        tablelist = filehandle.read().splitlines()
                    logging.debug("Tables are  {tablelist}")
                    print("\nExtracting ")
                    for eachtable in (tablelist):
                        self.createExtractOfTable(
                            tablename=eachtable, isSourceOrTarget=user_isSourceOrTarget)
                elif user_inp == 2:
                    user_isSourceOrTarget = input(
                        '''Which DB tables you want to extract ? Enter 'S' for Source or 'T' for Target (without single qoutes obviosly) \n
                             |-->''')
                    logging.debug(f"User Input is {user_isSourceOrTarget}")
                    if (user_isSourceOrTarget != 'S' and user_isSourceOrTarget != 'T'):
                        exit(-1)
                    usr_table = input("Insert Table Name  -> ")
                    print("Starting Extract of Table -> ", usr_table)
                    print("Connecting To Relevant Database ...")
                    self.get_connection(sourceOrTargetDB=user_isSourceOrTarget)
                    self.createExtractOfTable(
                        tablename=usr_table, isSourceOrTarget=user_isSourceOrTarget)
                elif user_inp == 3:
                    print("-----------------------------------------")
                    print("\n")
                    print("Available Directories are  as Follows : ")
                    for i in next(os.walk('.'))[1]:
                        print(i)
                    print("-----------------------------------------")
                    print("\n")
                    s_dir=input("Enter Source Directory name from which you want to Compare  : ")
                    t_dir=input("Enter Target Directory name to which you want to Compare  : ")
                    if len(s_dir)==0 and len(t_dir)==0:
                        print("Invalid Directory Given ")
                        exit(-2)
                    print("\nStarting Comparison Please Wait....\n")
                    try:
                        self.runThroughFiles(src_dir=s_dir,tar_dir=t_dir)
                    except Exception as ex:
                        print(ex)
                else:
                    print("Exiting")
                    exit(0)
            else:
                print("Please select a valid option")

        


obj = ExtractFromOracle()
obj.driveMenuOptions()
