import os
import pandas_gbq
import pandas as pd
import tkinter as tk

from PIL import Image, ImageTk
from google.cloud import bigquery
from tkinter import filedialog, Text

# Starts the GUI App

root = tk.Tk()

# Setting the title of our window

root.title("Airlift")

# Using canvas to enlarge the UI window

canvas = tk.Canvas(root, width = 1000, height = 700)
canvas.grid(columnspan = 3, rowspan = 22)

# Space before logo 

space1 = tk.Label(root, text = " ", font = ("raleway", 16))
space1.grid(column = 1, row = 0)

# Airlift logo

logo = Image.open('logo.png')
logo = ImageTk.PhotoImage(logo)

logo_label = tk.Label(image = logo)
logo_label.image = logo
logo_label.grid(column = 1, row = 1)

# Space after logo

space2 = tk.Label(root, text = " ", font = ("raleway", 16))
space2.grid(column = 1, row = 2)

# Getting Inputs from the user

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

# Function to move file to the cloud and validate source file and cloud table

def move_file_to_cloud():
    migrate2['text'] = "Click to start migration"
    migrate2['fg'] = "red"

    space6['text'] = " "

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

    global query_str

    if t.get() != "":
        table_name =  d.get() + "." + t.get()
        query_str = "select * from " + project_id + "." + table_name
    else:
        src_filename = source_file.split("/")[-1]
        table_name =  d.get() + "." + src_filename[:-4]
        query_str = "select * from " + project_id + "." + table_name

    print("\nTable name: ", table_name)
    print("\nQuery created with inputs: ", query_str, "\n")

    df.to_gbq(table_name, project_id, location_id, if_exists = 'replace')
    
    migrate['text'] = "Migration Complete"
    migrate['fg'] = "green"


    client = bigquery.Client(project_id)

    dataframe = (
        client.query(query_str)
        .result()
        .to_dataframe(
        create_bqstorage_client = True,
        )
    )

    global cloud_shape
    cloud_shape = dataframe.shape
    
    print("\nCloud dataframe: ", dataframe)
    print("\nCloud Shape: ", cloud_shape)
    
    global val_result

    if source_shape == cloud_shape:
        success_text = "The data file has been uploaded to the cloud successfully"
        source_row_col = "\nThe Source data has: " + str(source_shape[0]) + " rows " + "& " + str(source_shape[1]) + " columns"
        cloud_row_col = "\nThe Cloud table has: " + str(cloud_shape[0]) + " rows " + "& " + str(cloud_shape[1]) + " columns"
        val_result = success_text + source_row_col + cloud_row_col
        space4['text'] = val_result
        space4['fg'] = "green"
        print("\nValidation Result: ", val_result)

    else:
        val_result = "Oops!! The data file has not been uploaded to the cloud"
        space4['text'] = val_result
        space4['fg'] = "red"
        print("\nValidation Result: ", val_result)
    

# Function to move folder to the cloud and validate all files and cloud tables

def move_folder_to_cloud():
    migrate['text'] = "Click to start migration"
    migrate['fg'] = "red"

    space4['text'] = " "

    source_folder = filedialog.askdirectory(initialdir = '/', title = 'Select File')
    print("\nSource folder: ", source_folder)

    data_files = os.listdir(source_folder)
    data_files = [file for file in data_files if file[-4:] == ".csv"]
    
    print("\nData files: ", data_files)
    print("\nNum of Data files: ", len(data_files))

    project_id = p.get()
    location_id = 'us-central1'

    global count
    count = 0

    for files in data_files:
        df = pd.read_csv(os.path.join(source_folder, files))
        print("\ndf: ", df)

        global file_shape
        file_shape = df.shape
        print("\nFile Shape: ", file_shape)
        
        table_name =  d.get() + "." + files[:-4]
        print("\nTable name: ", table_name)

        global query_str
        query_str = "select * from " + project_id + "." + table_name
        print("\nQuery created with inputs: ", query_str, "\n")
    
        df.to_gbq(table_name, project_id, location_id, if_exists = 'replace')

        client = bigquery.Client(project_id)

        dataframe = (
            client.query(query_str)
            .result()
            .to_dataframe(
            create_bqstorage_client = True,
            )
        )

        global cloud_shape
        cloud_shape = dataframe.shape
    
        print("\nCloud dataframe: ", dataframe)
        print("\nCloud Shape: ", cloud_shape)

        if file_shape == cloud_shape:
            count += 1
        
        print("\nFile Count: ", count)

    migrate2['text'] = "Migration Complete"
    migrate2['fg'] = "green"

    global val_result

    if len(data_files) == count:
        success_text = "All data files have been uploaded to the cloud successfully"
        source_files = "\nThe Source folder contains: " + str(len(data_files)) + " files"
        cloud_files = "\nThe Cloud Dataset contains: " + str(count) + " tables"
        final_text = "\nThe rows and columns count are equal for all the source files and cloud tables"
        val_result = success_text + source_files + cloud_files +  final_text
        space6['text'] = val_result
        space6['fg'] = "green"
        print("\nValidation Result: ", val_result)

    else:
        val_result = "Oops!! The data files in the folder has not been uploaded to the cloud"
        space6['text'] = val_result
        space6['fg'] = "red"
        print("\nValidation Result: ", val_result)


space3 = tk.Label(root, text = " ", font = ("raleway", 16))
space3.grid(column = 1, row = 9)

# Text to Select a file

select = tk.Label(root, text = "Select a CSV File to migrate to GCP Big Query", font = ("raleway", 20))
select.grid(column = 1, row = 10)

# Design for the button to move file to cloud

migrate_text = tk.StringVar()
migrate_btn = tk.Button(root, textvariable = migrate_text, font = ("raleway", 20), 
                        height = 2, width = 20, borderwidth = 5, bg = "red", fg = "blue", command = move_file_to_cloud)

migrate_text.set("Migrate file to cloud")
migrate_btn.grid(column = 1, row = 11)

migrate = tk.Label(root, text = "Click to start migration", fg = "red", font = ("raleway", 16))
migrate.grid(column = 1, row = 12)

# Using this space to dynamically alter the text and print the validation message

space4 = tk.Label(root, text = " ", font = ("raleway", 16))
space4.grid(column = 1, row = 13)

# Additional space in between 

space5 = tk.Label(root, text = " ", font = ("raleway", 16))
space5.grid(column = 1, row = 14)

# Text to Select a folder

select2 = tk.Label(root, text = "Select a Folder to migrate all CSV files to GCP Big Query", font = ("raleway", 20))
select2.grid(column = 1, row = 15)

# # Design for the button to move folder to cloud

migrate2_text = tk.StringVar()
migrate2_btn = tk.Button(root, textvariable = migrate2_text, font = ("raleway", 20), 
                        height = 2, width = 20, borderwidth = 5, bg = "red", fg = "blue", command = move_folder_to_cloud)

migrate2_text.set("Migrate folder to cloud")
migrate2_btn.grid(column = 1, row = 16)

migrate2 = tk.Label(root, text = "Click to start migration", fg = "red", font = ("raleway", 16))
migrate2.grid(column = 1, row = 17)

# Using this space to dynamically alter the text and print the validation message

space6 = tk.Label(root, text = " ", font = ("raleway", 16))
space6.grid(column = 1, row = 18)

# Additional spacing at the end

space7 = tk.Label(root, text = " ", font = ("raleway", 16))
space7.grid(column = 1, row = 19)

space8 = tk.Label(root, text = " ", font = ("raleway", 16))
space8.grid(column = 1, row = 20)

# Extending our canvas in the UI window

canvas = tk.Canvas(root, width = 700, height = 100)
canvas.grid(columnspan = 3)

# Ends the GUI App

root.mainloop()