# Extract Raw Data
import sys, requests, datetime as dt, pandas as pd
sys.path.append('../')

class Extract:
    '''Pipeline to extract bytes data and parse into a table'''
    def __init__(self) -> None:
        self.data = None

    def raw_from_web(self, url: str = None):
        '''Extract raw data from web URL'''
        req = requests.get(url=url)

        if req.status_code != 200:
            raise ValueError(f'''Invalid URL ({req.status_code})''')
        else:
            self.data = req.content
        return self

    def convert_type(self, new: str = 'utf-8'):
        '''Convert data from current to new type'''
        self.data = str(self.data, encoding=new)
        return self

    def split(self, delim_list: list):
        '''Split string by delim'''
        self.data = [i.split(delim_list[1]) for i in self.data.split(delim_list[0])]
        return self

    def create_table(self):
        self.data = pd.DataFrame(data = self.data[1:], columns=self.data[0]).drop(columns=[''])
        return self

    def load_to_storage(self, filepath: str, filename:str):
        if not isinstance(self.data, pd.DataFrame):
            self.create_table()
        
        timestamp = f'''{dt.datetime.today().year}_{dt.datetime.today().month}_{dt.datetime.today().day}'''
        file = f'''{filename.split('.')[0]}_{timestamp}.{filename.split('.')[1]}'''
        full_path = f'''{filepath}{file}'''
        self.data.to_csv(path_or_buf=full_path)
        return self

if __name__ == '__main__':
    test = (
        Extract()
        .raw_from_web(url='https://raw.githubusercontent.com/DNYFZR/TennisApp/main/data/ATP_tour.csv')
        .convert_type()
        .split(delim_list=['\n', ','])
        .create_table()
        .load_to_storage(filepath='data/raw/', filename='ATP_tour.csv')
    )