import os
import shutil
import pandas_gbq
import pandas as pd
import tkinter as tk

from PIL import Image, ImageTk
from google.cloud import bigquery
from tkinter import filedialog, Text

root = tk.Tk()

#Using canvas to enlarge the UI window

canvas = tk.Canvas(root, width = 700, height = 500)
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
    print(source)
    src_file = source.split("/")[-1]
    cd_files = os.listdir()
    destn = os.path.abspath(cd_files[-1]).split("/")[:-1]
    destn = "/".join(destn) + "/" + src_file
    shutil.copy(source, destn)
    dst_file = destn.split("/")[-1]

    df = pd.read_csv(dst_file)
    print(df)

    project_id = p.get()
    table_name =  d.get() + "." + t.get()
    location_id = 'us-central1'

    df.to_gbq(table_name, project_id, location_id, if_exists = 'replace')

    qr = "select * from " + p.get() + "." + d.get() + "." + t.get()
    print(qr)

space1 = tk.Label(root, text = " ", font = "raleway")
space1.grid(column = 1, row = 7)

#select

select = tk.Label(root, text = "Select a File to migrate from Local storage to Cloud Big Query", font = "raleway")
select.grid(column = 1, row = 8)

# Read a csv file, convert it to a dataframe and migrate it to cloud

migrate_text = tk.StringVar()
migrate_btn = tk.Button(root, textvariable = migrate_text, 
                    font = 'raleway', height = 2, width = 15, bg = 'green', command = move_to_cloud)

migrate_text.set("Migrate data to cloud")
migrate_btn.grid(column = 1, row = 9)

migrate = tk.Label(root, text = "The file will be copied and migrated to the cloud", font = "raleway")
migrate.grid(column = 1, row = 10)

space2 = tk.Label(root, text = " ", font = "raleway")
space2.grid(column = 1, row = 11)

query = tk.Label(root, text = "Enter the query you want to read from the cloud", font = "raleway")
query.grid(column = 1, row = 12)

q = tk.Entry(root, width = 50, borderwidth = 5)
q.grid(column = 1, row = 13)

#"SELECT DISTINCT(super_line_split) FROM `gcp-ent-capstone-dev.TestTeam4.key_metrics_small`"

def read_from_cloud():
    query_str = q.get()
    client = bigquery.Client('gcp-ent-capstone-dev')

    dataframe = (
        client.query(query_str)
        .result()
        .to_dataframe(
        create_bqstorage_client=True,
        )
    )

    print(dataframe)

# Read the table from cloud as a dataframe in python

read_text = tk.StringVar()
read_btn = tk.Button(root, textvariable = read_text, 
                    font = "raleway", bg = "green", height = 2, width = 15, command = read_from_cloud)

read_text.set("Read data from Cloud")
read_btn.grid(column = 1, row = 14)

read = tk.Label(root, text = "The data read from cloud is displayed in the terminal", font = "raleway")
read.grid(column = 1, row = 15)

canvas = tk.Canvas(root, width = 700, height = 100)
canvas.grid(columnspan = 3)

root.mainloop()