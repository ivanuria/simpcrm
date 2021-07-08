import asyncio
import gettext
import tkinter as tk
from .main import Main
from .tkshare.frames import Frames

gettext.install("simpcrm")
_ = gettext.gettext # Explicit

class App():
    def __init__(self, main_app, config):
        self.config = config
        self.main_app = main_app
        self.loop = asyncio.new_event_loop()

    def install(self):
        root = tk.Tk()
        root.title(_("Installation of SimCRM"))
        install_frame = Frames.Install(root, self.main_app)
        root.mainloop()

    def mainloop(self):
        if not self.main_app.installed:
            self.install()

        

def main(config):
    print("Iniatilizing TKInter")
    main_app = Main(config=dict(config))
    app = App(main_app, config)
    app.mainloop()