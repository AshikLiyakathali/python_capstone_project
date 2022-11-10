import os
import shutil
import pandas_gbq
import pandas as pd
import tkinter as tk

from PIL import Image, ImageTk
from google.cloud import bigquery
from tkinter import filedialog, Text

root = tk.Tk()

root.title("Airlift")

#Using canvas to enlarge the UI window

canvas = tk.Canvas(root, width = 1000, height = 700)
canvas.grid(columnspan = 3, rowspan = 15)

#logo

logo = Image.open('logo.png')
logo = ImageTk.PhotoImage(logo)

logo_label = tk.Label(image = logo)
logo_label.image = logo
logo_label.grid(column = 1, row = 0)

# Inputs

pid = tk.Label(root, text = "Enter Project id", font = "raleway")
pid.grid(column = 1, row = 1)

p = tk.Entry(root, width = 30, borderwidth = 5)
p.grid(column = 1, row = 2)

did = tk.Label(root, text = "Enter Dataset id", font = "raleway")
did.grid(column = 1, row = 3)

d = tk.Entry(root, width = 30, borderwidth = 5)
d.grid(column = 1, row = 4)

table = tk.Label(root, text = "Enter table name", font = "raleway")
table.grid(column = 1, row = 5)

t = tk.Entry(root, width = 30, borderwidth = 5)
t.grid(column = 1, row = 6)

# Function to move file to the cloud

def move_to_cloud():
    source =filedialog.askopenfilename(initialdir = '/', title = 'Select File', 
                                         filetypes = (("csv", "*.csv"), ("all files", "*.*")))
    src_file = source.split("/")[-1]
    cd_files = os.listdir()
    destn = os.path.abspath(cd_files[-1]).split("/")[:-1]
    destn = "/".join(destn) + "/" + src_file
    shutil.copy(source, destn)
    dst_file = destn.split("/")[-1]

    df = pd.read_csv(dst_file)
    
    global source_shape
    source_shape = df.shape
    print("Source Shape: ", source_shape)

    project_id = p.get()
    table_name =  d.get() + "." + t.get()
    location_id = 'us-central1'

    df.to_gbq(table_name, project_id, location_id, if_exists = 'replace')
    
    migrate['text'] = "Migration status: Complete"
    migrate['fg'] = "green"

    global query
    query = "select * from " + p.get() + "." + d.get() + "." + t.get()
    print("Query created with inputs: ", query)


space1 = tk.Label(root, text = " ", font = "raleway")
space1.grid(column = 1, row = 7)

#select

select = tk.Label(root, text = "Select a File to migrate from Local storage to Cloud Big Query", font = "raleway")
select.grid(column = 1, row = 8)

# Read a csv file, convert it to a dataframe and migrate it to cloud

migrate_text = tk.StringVar()
migrate_btn = tk.Button(root, textvariable = migrate_text, font = 'raleway', 
                        height = 2, width = 15, command = move_to_cloud)

migrate_text.set("Migrate data to cloud")
migrate_btn.grid(column = 1, row = 9)

migrate = tk.Label(root, text = "Migration status: Click to start", fg = "red", font = "raleway")
migrate.grid(column = 1, row = 10)

space2 = tk.Label(root, text = " ", font = "raleway")
space2.grid(column = 1, row = 11)

valid = tk.Label(root, text = "Click the button to validate the data uploaded to the cloud", font = "raleway")
valid.grid(column = 1, row = 12)

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
        val_result = "The file has been uploaded to the cloud successfully" + "\nThe table has: " + str(cloud_shape[0]) + " rows " + "& " + str(cloud_shape[1]) + " columns"
        validate['text'] = val_result
        validate['fg'] = "green"
        print(val_result)

    else:
        val_result = "Oops!! The file has not been uploaded to the cloud"
        validate['text'] = val_result
        validate['fg'] = "red"
        print(val_result)

# Read the table from cloud as a dataframe in python

validate_text = tk.StringVar()
validate_btn = tk.Button(root, textvariable = validate_text, font = "raleway",
                    height = 2, width = 15, command = validate_from_cloud)

validate_text.set("Validate data from Cloud")
validate_btn.grid(column = 1, row = 14)

validate = tk.Label(root, text = " ", font = "raleway")
validate.grid(column = 1, row = 15)

canvas = tk.Canvas(root, width = 700, height = 100)
canvas.grid(columnspan = 3)

root.mainloop()