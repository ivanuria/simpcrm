"""Module to define frames for play with tkinter
They Will be accesible throught Frames enumerate
"""

import tkinter as tk
from enum import Enum


class InstallFrame(tk.Frame):
    def __init__(self, master, main_app):
        super().__init__(master)
        self.main_app = main_app
        self.master = master

class Frames(Enum):
    Install = InstallFrame