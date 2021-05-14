# databases.py

class Data(list): #not checked datatypes
    def __init__(self, data):
        if isinstance(data, dict):
            data = [data]
        super().__init__(data)

class DBInterface:
    def __init__(self):
        pass

    @property
    def database(self):
        pass

    @property
    def table(self):
        pass

    @property
    def filter(self):
        pass

    def connect(self):
        pass

    def disconnect(self):
        pass

    def set_database(self, name):
        pass

    def set_table(self, name):
        pass

    def set_filter(self, filter):
        pass

    def insert(self, data):
        pass

    def update(self, data, filter=None):
        pass

    def select(self, filter=None):
        pass

    def remove(self, filter=None):
        pass
