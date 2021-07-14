"""Helper to set new classes for tkinter and make it easier.
"""

from datetime import datetime
from entities.entities import Item
from typing import NoReturn
import tkinter as tk


class ItemWrapper(dict):
    """For wrapping an Item and returning tkinter types
    It overrides dict making it mover easy to use.
    Arguments:
        item: Item to wrap
        config: parsed configuration
    """
    def __init__(self, item:Item, config:dict) -> NoReturn:
        super().__init__()
        self.item = item
        self._keys = list(item.entity.fields.keys())
        self._types = list(item.entity.fields.values())
        primary_index = self._keys.index(self.item.primary_key)
        #Primaryindex is always a list [type, DBEnum.PRIMARY]
        self._types[primary_index] = self._types[primary_index][0]
        dt_format = " ".join(config["Formats"]["date"], config["Formats"]["time"])
        types = {
        # It will be a tuple with three values: [tkinter, getter, setter]
        # Both getter and setter would be lambdas for conversion if neccesary
            int: [tk.IntVar, lambda x: x, lambda x: x],
            str: [tk.StringVar, lambda x: x, lambda x: x],
            datetime: [tk.StringVar, lambda x: x.strftime(dt_format), lambda x: datetime.strptime(x, dt_format)],
            float: [tk.DoubleVar, lambda x: x, lambda x: x],
            bool: [tk.BooleanVar, lambda x: x, lambda x: x]
        }
        for i, key in enumerate(self._keys):
            variable, getter, setter = types[self._types[i]]
            variable.trace_add("w", lambda name, *_, key=key: self.item.changed_handler(key)(setter(tk.Variable(name).get())))
            self.item.set_handler(key, lambda x: variable.set(getter(x)))
            super().__setitem__(key, variable)
