import pyodbc
import pandas as pd
import os
import PyPDF2
import re
import shutil
import fnmatch
from sqlalchemy import create_engine
import fitz
import json



current_path = os.path.dirname(os.path.abspath(__file__))
configpath = os.path.join(current_path, 'config.json')
with open(configpath, 'r') as config_file:
    config = json.load(config_file)




class DatabaseConnection:
    def __init__(self, connection_string):
        self.engine = create_engine(connection_string)
        try:
            self.conn = self.engine.connect()
            print("Connection established successfully.")
        except Exception as e:
            print(f"Error connecting to the database: {e}")
    def close_connection(self):
        self.conn.close()


def process_folder(db_conn1, db_conn2, folderpath, folder_name):


    folder_path_full = os.path.join(folderpath, folder_name)

    # Determine the 'Amount' value for this folder
    if folder_name == 'Back-up for everything' or folder_name == 'Physical Mailing':
        amount = 0
    else:
        dollarpattern = r'\$([1-9]\d*\.\d+|\d{2,}(?=\D|$))'
        match = re.search(dollarpattern, folder_name)
        amount = float(match.group(1)) if match else 0

    list_pdf = [f for f in os.listdir(folder_path_full) if f.endswith(".pdf")]

    for pdf_file in list_pdf:
        pdf_path = os.path.join(folder_path_full, pdf_file)

        # Extract the invoice number from the PDF
        invoice_number = extract_invoice_number(pdf_path)

        if invoice_number:
            # Create a subfolder for each invoice
            invoice_folder = os.path.join(folder_path_full, invoice_number)

            # Move the PDF file to the invoice folder
            move_pdf_to_invoice_folder(pdf_path, invoice_folder)

            # Process attachments based on your requirements
            process_attachments(db_conn1, db_conn2, invoice_number, amount, invoice_folder)
        else:
            print(f"Invoice number not found in {pdf_path}")


def find_files(start_dir, pattern):
    for root, dirs, files in os.walk(start_dir):
        for file_name in fnmatch.filter(files, pattern):
            yield os.path.join(root, file_name)

def split_pdf_pages_with_keyword(input_pdf, codekeyword, finalcodepdf):
    pdf_document = fitz.open(input_pdf)
    for page_num in range(len(pdf_document)):
        page = pdf_document[page_num]
        text = page.get_text()
        if codekeyword in text:
            output_document = fitz.open()
            output_document.insert_pdf(pdf_document, from_page=page_num, to_page=page_num)
            output_path = f"{finalcodepdf}.pdf"
            output_document.save(output_path)
            output_document.close()
    pdf_document.close()

def extract_invoice_number(pdf_path):
    pdf_access = (open(pdf_path, "rb"))
    pdf_reader = PyPDF2.PdfReader(pdf_access)
    content = pdf_reader._get_page(0).extract_text()
    pattern = r"Invoice No\. (\d+)"
    matches = re.findall(pattern, content)
    pdf_access.close()
    return matches[0] if matches else None

def move_pdf_to_invoice_folder(pdf_path,  destination_folder):
    if not os.path.isdir(destination_folder):
        os.mkdir(destination_folder)
    movefile = os.path.join(destination_folder, os.path.basename(pdf_path)) ###
    shutil.move(pdf_path, movefile)

def process_attachments(db_conn1, db_conn2, invoice_number, amount, invoice_folder):
    # SQL Query for 3e Attachments
    config["sql"] = config["sql"].format(Cinvoice_number = invoice_number, Camount = amount)
    sql = config['sql']
    df = pd.DataFrame()
    df = pd.read_sql(sql, db_conn1.conn)
    tempdata = []

    for index, row in df.iterrows():
        if row['FileName'] == 'NULL' and row['payee'] != '17692' and row['HasAttach'] == 0:
            tempdata.append({
                "Invoice": invoice_number,
                "Vendor": row['payee'],
                "Vendor_Invoice": row['Vendor_Invoice'],
                "Amount": amount,
                "Processed_By": row['BaseUserName'],
                "Status": "Unattached"
            })
        elif row['FileName'] == 'NULL' and row['payee'] == '17692':
            start_directory = config['start_directory_nationwide']
            vendor_invoice = row['Vendor_Invoice']
            pattern = f"*{vendor_invoice}*"
            found_file = next(find_files(start_directory, pattern), None)

            if found_file:
                codepattern = re.compile(r'[A-Z]{2}\d{4,}')
                findingthecode = row['Narrative']
                match = re.search(codepattern, findingthecode)
                if match:
                    firstmatch = match.group()
                    tempdata.append({
                        "Invoice": invoice_number,
                        "Vendor": row['payee'],
                        "Vendor_Invoice": row['Vendor_Invoice'],
                        "Amount": amount,
                        "Processed_By": row['BaseUserName'],
                        "Status": "Split_Remaining"
                    })
                else:
                    tempdata.append({
                        "Invoice": invoice_number,
                        "Vendor": row['payee'],
                        "Vendor_Invoice": row['Vendor_Invoice'],
                        "Amount": amount,
                        "Processed_By": row['BaseUserName'],
                        "Status": "Split_Remaining"
                    })

                codekeyword = firstmatch
                nationwideoutput = row['Vendor_Invoice'] + "_" + codekeyword
                finalcodepdf = os.path.join(invoice_folder, nationwideoutput)
                split_pdf_pages_with_keyword(found_file, codekeyword, finalcodepdf)
            else:
                tempdata.append({
                    "Invoice": invoice_number,
                    "Vendor": row['payee'],
                    "Vendor_Invoice": row['Vendor_Invoice'],
                    "Amount": amount,
                    "Processed_By": row['BaseUserName'],
                    "Status": "file_not_found"
                })
        elif row['FileName'] != 'NULL':
            tempdata.append({
                "Invoice": invoice_number,
                "Vendor": row['payee'],
                "Vendor_Invoice": row['Vendor_Invoice'],
                "Amount": amount,
                "Processed_By": row['BaseUserName'],
                "Status": "Backup_Done"
            })
            
            start_directory = config['start_directory_voucher']
            filename = row['FileName']
            pattern = f"*{filename}"
            found_file = next(find_files(start_directory, pattern), None)

            if found_file:
                shutil.copy(found_file, invoice_folder)

        elif row['FileName'] == 'NULL' and row['HasAttach'] == 1:
            tempdata.append({
                "Invoice": invoice_number,
                "Vendor": row['payee'],
                "Vendor_Invoice": row['Vendor_Invoice'],
                "Amount": amount,
                "Processed_By": row['BaseUserName'],
                "Status": "Backup_Done"
            })
            config["sql2"] = config["sql2"].format(CVchrIndex=row['VchrIndex'])
            sql2 = config['sql2']
            df2 = pd.DataFrame()
            df2 = pd.read_sql(sql2, db_conn2.conn)

            if not df2.empty:
                icfile = df2['Icfilepath'][0]
                shutil.copy(icfile, invoice_folder)

    logexcel = pd.DataFrame(tempdata)
    exlname = os.path.join(invoice_folder, "log_excel.xlsx")
    logexcel.to_excel(exlname)
    keywords = ['file_not_found', 'Split_Remaining', 'Unattached', 'Backup_Done']
    print('the file is prepared')
    for keyword in keywords:
        keymatches = logexcel['Status'].str.contains(keyword, case=False, na=False)
    if keymatches.any() == True:
        Incompleterename = invoice_folder + "_Incomplete"
        os.rename(invoice_folder, Incompleterename)
    else:
        completerename = invoice_folder + "_Complete"
        os.rename(invoice_folder, completerename)


def main():
    # Define your connection strings
    connection_string1 = config['connection_string1']
    connection_string2 = config['connection_string2']

    # Create database connections
    db_conn1 = DatabaseConnection(connection_string1)
    db_conn2 = DatabaseConnection(connection_string2)

    folderpath = config['folderpath']

    for f in os.listdir(folderpath):
        if os.path.isdir(os.path.join(folderpath, f)):
            process_folder(db_conn1, db_conn2, folderpath, f)

    # Close database connections
    db_conn1.close_connection()
    db_conn2.close_connection()


    

if __name__ == "__main__":
    main()
