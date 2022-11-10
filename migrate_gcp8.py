import os
import csv
import shutil
import pandas_gbq
import pandas as pd
import tkinter as tk

from PIL import Image, ImageTk
from google.cloud import bigquery
from tkinter import filedialog, Text

def process_csv(filename):
    example_file = open(filename, encoding="utf-8")
    example_reader = csv.reader(example_file)
    example_data = list(example_reader)
    example_file.close()
    return example_data

root = tk.Tk()

root.title("Airlift")

#Using canvas to enlarge the UI window

canvas = tk.Canvas(root, width = 1000, height = 700)
canvas.grid(columnspan = 3, rowspan = 20)

# Space before logo 

space1 = tk.Label(root, text = " ", font = "raleway")
space1.grid(column = 1, row = 0)

#logo

logo = Image.open('logo.png')
logo = ImageTk.PhotoImage(logo)

logo_label = tk.Label(image = logo)
logo_label.image = logo
logo_label.grid(column = 1, row = 1)

# Space after logo

space1 = tk.Label(root, text = " ", font = "raleway")
space1.grid(column = 1, row = 2)

# Inputs

pid = tk.Label(root, text = "Enter Project id", font = ("raleway", 20))
pid.grid(column = 1, row = 3)

p = tk.Entry(root, width = 30, borderwidth = 5, font = ("raleway", 20))
p.grid(column = 1, row = 4)

did = tk.Label(root, text = "Enter Dataset id", font = ("raleway", 20))
did.grid(column = 1, row = 5)

d = tk.Entry(root, width = 30, borderwidth = 5, font = ("raleway", 20))
d.grid(column = 1, row = 6)

table = tk.Label(root, text = "Enter Table name", font = ("raleway", 20))
table.grid(column = 1, row = 7)

t = tk.Entry(root, width = 30, borderwidth = 5, font = ("raleway", 20))
t.grid(column = 1, row = 8)

# Function to move file to the cloud

def move_file_to_cloud():
    source_file = filedialog.askopenfilename(initialdir = '/', title = 'Select File', 
                                         filetypes = (("csv", "*.csv"), ("all files", "*.*")))
    print("\nSource file: ", source_file)

    df = pd.read_csv(source_file)
    print("\nSource df: ", df)

    global source_shape
    source_shape = df.shape
    print("\nSource Shape: ", source_shape)

    project_id = p.get()
    location_id = 'us-central1'

    global query

    if t.get() != "":
        table_name =  d.get() + "." + t.get()
        query = "select * from " + p.get() + "." + table_name
    else:
        src_filename = source_file.split("/")[-1]
        table_name =  d.get() + "." + src_filename[:-4]
        query = "select * from " + p.get() + "." + table_name

    print("\nTable name: ", table_name)
    print("\nQuery created with inputs: ", query)

    #df.to_gbq(table_name, project_id, location_id, if_exists = 'replace')
    
    migrate['text'] = "Migration status: Complete"
    migrate['fg'] = "green"

# Function to move folder to the cloud

def move_folder_to_cloud():
    source_folder = filedialog.askdirectory(initialdir = '/', title = 'Select File')
    print("Source folder: ", source_folder)

    data_files = os.listdir(source_folder)
    data_files = [file for file in data_files if file[-4:] == ".csv"]
    
    print("Data file: ", data_files)

    project_id = p.get()
    location_id = 'us-central1'

    for files in data_files:
        df = pd.read_csv(os.path.join(source_folder, files))
        print("\ndf: ", df)

        global source_shape
        file_shape = df.shape
        print("\nFile Shape: ", file_shape)
        
        table_name =  d.get() + "." + files[:-4]
        print("\nTable name: ", table_name)
    
        df.to_gbq(table_name, project_id, location_id, if_exists = 'replace')
    
    migrate2['text'] = "Migration status: Complete"
    migrate2['fg'] = "green"

space1 = tk.Label(root, text = " ", font = "raleway")
space1.grid(column = 1, row = 9)

#select a file

select = tk.Label(root, text = "Select a CSV File to migrate from Local storage to GCP Big Query", font = ("raleway", 20))
select.grid(column = 1, row = 10)

# Read a csv file, convert it to a dataframe and migrate it to cloud

migrate_text = tk.StringVar()
migrate_btn = tk.Button(root, textvariable = migrate_text, font = ("raleway", 20), 
                        height = 2, width = 20, borderwidth = 5, bg = "red", command = move_file_to_cloud)

migrate_text.set("Migrate file to cloud")
migrate_btn.grid(column = 1, row = 11)

migrate = tk.Label(root, text = "Migration status: Click to start", fg = "red", font = ("raleway", 16))
migrate.grid(column = 1, row = 12)

space2 = tk.Label(root, text = " ", font = ("raleway", 20))
space2.grid(column = 1, row = 13)

#select a folder

select2 = tk.Label(root, text = "Select a Folder to migrate all CSV files to GCP Big Query", font = ("raleway", 20))
select2.grid(column = 1, row = 14)

# Read all csv files from a folder, convert it to a dataframe and migrate it to cloud

migrate2_text = tk.StringVar()
migrate2_btn = tk.Button(root, textvariable = migrate2_text, font = ("raleway", 20), 
                        height = 2, width = 20, borderwidth = 5, bg = "red", command = move_folder_to_cloud)

migrate2_text.set("Migrate folder to cloud")
migrate2_btn.grid(column = 1, row = 15)

migrate2 = tk.Label(root, text = "Migration status: Click to start", fg = "red", font = ("raleway", 16))
migrate2.grid(column = 1, row = 16)

space2 = tk.Label(root, text = " ", font = ("raleway", 20))
space2.grid(column = 1, row = 17)

# valid = tk.Label(root, text = "Click the button to validate the data uploaded to the cloud", font = ("raleway", 20))
# valid.grid(column = 1, row = 18)

def validate_from_cloud():
    query_str = query
    client = bigquery.Client('gcp-ent-capstone-dev')

    dataframe = (
        client.query(query_str)
        .result()
        .to_dataframe(
        create_bqstorage_client=True,
        )
    )

    global cloud_shape
    cloud_shape = dataframe.shape
    
    print("Cloud dataframe: ", dataframe)
    print("Cloud Shape: ", cloud_shape)
    
    global val_result

    if source_shape == cloud_shape:
        success_text = "The file has been uploaded to the cloud successfully"
        source_row_col = "\nThe Source data has: " + str(source_shape[0]) + " rows " + "& " + str(source_shape[1]) + " columns"
        cloud_row_col = "\nThe Cloud table has: " + str(cloud_shape[0]) + " rows " + "& " + str(cloud_shape[1]) + " columns"
        val_result = success_text + source_row_col + cloud_row_col
        validate['text'] = val_result
        validate['fg'] = "green"
        print(val_result)

    else:
        val_result = "Oops!! The file has not been uploaded to the cloud"
        validate['text'] = val_result
        validate['fg'] = "red"
        print(val_result)

# Read the table from cloud as a dataframe in python and validate it

# validate_text = tk.StringVar()
# validate_btn = tk.Button(root, textvariable = validate_text, font = ("raleway", 20),
#                     height = 2, width = 15, command = validate_from_cloud)

# validate_text.set("Validate data from Cloud")
# validate_btn.grid(column = 1, row = 19)

# validate = tk.Label(root, text = " ", font = ("raleway", 16))
# validate.grid(column = 1, row = 20)

canvas = tk.Canvas(root, width = 700, height = 100)
canvas.grid(columnspan = 3)

root.mainloop()