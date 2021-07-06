import tkinter as tk
from .main import Main

class App(tk.Frame):
    def __init__(self, main_app, master=None):
        super().__init__(master)
        self.master = master
        self.main_app = main_app
        self.pack()

def main(config):
    print("Iniatiliing TKInter")
    main_app = Main(config=dict(config))
    root = tk.Tk()
    app = App(main_app, master=root)
    app.mainloop()