import os
import shutil
import pandas_gbq
import pandas as pd
import tkinter as tk

from PIL import Image, ImageTk
from google.cloud import bigquery
from tkinter import filedialog, Text

def move_to_cloud():
    source =filedialog.askopenfilename(initialdir = '/', title = 'Select File', 
                                         filetypes = (("csv", "*.csv"), ("all files", "*.*")))
    print(source)
    src_file = source.split("/")[-1]
    cd_files = os.listdir()
    destn = os.path.abspath(cd_files[-1]).split("/")[:-1]
    destn = "/".join(destn) + "/" + src_file
    shutil.copy(source, destn)
    dst_file = destn.split("/")[-1]

    df = pd.read_csv(dst_file)
    print(df)

    project_id = 'gcp-ent-capstone-dev'
    dataset_id = 'TestTeam4'
    table_name =  dataset_id + "." + dst_file[:-4]
    location_id = 'us-east1'

    df.to_gbq(table_name, project_id, location_id, if_exists = 'replace')

def read_from_cloud(query_str = "SELECT * FROM `gcp-ent-capstone-dev.TestTeam4.key_metrics_small`"):
    client = bigquery.Client('gcp-ent-capstone-dev')

    dataframe = (
        client.query(query_str)
        .result()
        .to_dataframe(
        create_bqstorage_client=True,
        )
    )

    print(dataframe)

root = tk.Tk()

#Using canvas to enlarge the UI window

canvas = tk.Canvas(root, width = 700, height = 500)
canvas.grid(columnspan = 3, rowspan = 10)

#logo

logo = Image.open('logo.png')
logo = ImageTk.PhotoImage(logo)

logo_label = tk.Label(image = logo)
logo_label.image = logo
logo_label.grid(column = 1, row = 0)

#instrucitons

instructions = tk.Label(root, text = "Select a File to migrate from Local storage to Cloud Big Query", font = "raleway")
instructions.grid(columnspan = 3, column = 0, row = 1)

# Read a csv file, convert it to a dataframe and migrate it to cloud

migrate_text = tk.StringVar()
migrate_btn = tk.Button(root, textvariable = migrate_text, 
                    font = "raleway", bg = "green", height = 2, width = 15, command = move_to_cloud)

migrate_text.set("Migrate data to cloud")
migrate_btn.grid(column = 1, row = 2)

migrate = tk.Label(root, text = "The file will be copied and migrated to the cloud successfully", font = "raleway")
migrate.grid(column = 1, row = 3)

# Read the table from cloud as a dataframe in python

read_text = tk.StringVar()
read_btn = tk.Button(root, textvariable = read_text, 
                    font = "raleway", bg = "green", height = 2, width = 15, command = read_from_cloud)

read_text.set("Read data from Cloud")
read_btn.grid(column = 1, row = 4)

read = tk.Label(root, text = "The data read from cloud is displayed in the terminal", font = "raleway")
read.grid(column = 1, row = 5)

canvas = tk.Canvas(root, width = 700, height = 100)
canvas.grid(columnspan = 3)

root.mainloop()
