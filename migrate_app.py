import os
import shutil
import tkinter as tk

from tkinter import filedialog, Text

root = tk.Tk()

def select_file():
    
#     for widget in frame.winfo_children():
#         widget.destroy()
        
    filename = filedialog.askopenfilename(initialdir = '/', title = 'Select File', 
                                         filetypes = (("csv", "*.csv"), ("all files", "*.*")))

    print(filename)
    
    label = tk.Label(frame, text = "The location is : \n" + filename, bg = 'grey')
    label.pack()
        
canvas = tk.Canvas(root, height = 700, width = 700, bg = "grey")
canvas.pack()

frame = tk.Frame(root, bg = 'white')
frame.place(relwidth = 0.8, relheight = 0.8, relx = 0.1, rely = 0.1)

selectSrcFile = tk.Button(root, text = "Select Source File", padx = 10, 
                     pady = 5, fg= 'white', bg = 'red', command = select_file)

selectSrcFile.pack()

selectDst = tk.Button(root, text = "Select Destination", padx = 10, 
                     pady = 5, fg= 'white', bg = 'red', command = select_file)

selectDst.pack()

runApps = tk.Button(root, text = "Run Apps", padx = 10, 
                    pady = 5, fg= 'white', bg = 'grey')

runApps.pack()


root.mainloop()