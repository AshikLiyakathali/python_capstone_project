import os
import csv
import shutil
import tkinter as tk

from PIL import Image, ImageTk
from tkinter import filedialog, Text

def process_csv(filename):
    example_file = open(filename, encoding="utf-8")
    example_reader = csv.reader(example_file)
    example_data = list(example_reader)
    example_file.close()
    return example_data

def copy_file(file_name = 'key_metrics.csv'):
    
    source = '/Users/axl115/Documents/Digital Builder/Capstone/Migration/Location1'
    destination = '/Users/axl115/Documents/Digital Builder/Capstone/Migration/Location2'
    
    source_file =  os.path.join(source,file_name)
    destination_file = os.path.join(destination,file_name)
    
    if os.path.exists(source_file):  
        
        shutil.copy(source_file, destination_file)
        
        file1 = process_csv(source_file)
        file1_length = len(file1)

        file2 = process_csv(destination_file)
        file2_length = len(file2)
        
        if file1_length == file2_length:
            print("\nThe file has been copied and migrated successfully")
            
        else:
            print("\nError!! The file has not been migrated!!")
        
    else:
        print("\nError!! File was not found in source location!!")


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

instructions = tk.Label(root, text = "Select a File to migrate from Source to Destination", font = "raleway")
instructions.grid(columnspan = 3, column = 0, row = 1)

#Function to assign to button


# Source text and button

source_text = tk.StringVar()
source_btn = tk.Button(root, textvariable = source_text, 
                    font = "raleway", bg = "green", height = 2, width = 15)

source_text.set("Select Source")
source_btn.grid(column = 1, row = 2)


src = tk.Label(root, text = "Text for Source", font = "raleway")
src.grid(column = 1, row = 3)

# Destination text and button
destn_text = tk.StringVar()
destn_btn = tk.Button(root, textvariable = destn_text, 
                    font = "raleway", bg = "green", height = 2, width = 15)

destn_text.set("Select Destination")
destn_btn.grid(column = 1, row = 4)


dest = tk.Label(root, text = "Text for Destination", font = "raleway")
dest.grid(column = 1, row = 5)

# Migrate text and button

migrate_text = tk.StringVar()
migrate_btn = tk.Button(root, textvariable = migrate_text, 
                    font = "raleway", bg = "green", height = 2, width = 15, command = copy_file)

migrate_text.set("Migrate")
migrate_btn.grid(column = 1, row = 6)

migrate = tk.Label(root, text = "The file has been copied and migrated successfully", font = "raleway")
migrate.grid(column = 1, row = 7)


canvas = tk.Canvas(root, width = 700, height = 100)
canvas.grid(columnspan = 3)

root.mainloop()
