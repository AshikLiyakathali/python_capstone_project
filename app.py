import os
import tkinter as tk

from tkinter import filedialog, Text
from PIL import Image, ImageTk
from tkinter.filedialog import askopenfile

root = tk.Tk()

#Using canvas to enlarge the UI window

canvas = tk.Canvas(root, width = 700, height = 500)
canvas.grid(columnspan = 3, rowspan = 3)

#logo

logo = Image.open('logo.png')
logo = ImageTk.PhotoImage(logo)

logo_label = tk.Label(image = logo)
logo_label.image = logo
logo_label.grid(column = 1, row = 0)

#instrucitons

instructions = tk.Label(root, text = "Select PDF File to extract text", font = "raleway")
instructions.grid(columnspan = 3, column = 0, row = 1)

#Function to assign to button

# def open_file():
#     browse_text.set("loading...")
#     file = askopenfile(parent = root, mode = "rb", title = "Choose a file", filetype = [("PDF file", "*.pdf")])
#     if file:
#         print("file load success")


#Browse button

browse_text = tk.StringVar()
browse_btn = tk.Button(root, textvariable = browse_text, 
                    font = "raleway", bg = "green", height = 2, width = 15)

browse_text.set("Browse")
browse_btn.grid(column = 1, row = 2)

canvas = tk.Canvas(root, width = 700, height = 500)
canvas.grid(columnspan = 3)

root.mainloop()
