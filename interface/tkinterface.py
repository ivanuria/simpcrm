import asyncio
import gettext
import tkinter as tk
from .main import Main

gettext.install("simpcrm")

class InstallFrame(tk.Frame):
    def __init__(self, master, main_app):
        super().__init__(master)
        self.main_app = main_app
        self.master = master

class App():
    def __init__(self, main_app, config):
        self.config = config
        self.main_app = main_app
        self.loop = asyncio.new_event_loop()

    def install(self):
        root = tk.Tk()
        root.title(_("Installation of SimCRM"))
        install_frame = InstallFrame(root, self.main_app)
        root.mainloop()

    def mainloop(self):
        if not self.main_app.installed:
            self.install()

        

def main(config):
    print("Iniatilizing TKInter")
    main_app = Main(config=dict(config))
    app = App(main_app, config)
    app.mainloop()