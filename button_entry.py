import tkinter as tk
from tkinter import *

root = tk.Tk()

e = tk.Entry(root, width = 50, borderwidth = 5)
e.pack()

def myClick():
    hello = "Hello " + e.get()
    myLabel = tk.Label(root, text = hello)
    myLabel.pack()

myButton = tk.Button(root, text = "Enter your name", command = myClick)
myButton.pack()

root.mainloop()