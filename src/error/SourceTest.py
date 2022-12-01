import os, requests, pandas as pd, datetime as dt
from requests.exceptions import ConnectionError

class Processor:
    def __init__(self, type) -> None:
        self.type = type
        self.urls = {}
    
    @staticmethod 
    def source_test(source_name: str, url: str, log_filepath: str, accepted_codes: list = [200, ]):
        
        # Test connection & status
        try: requests.get(url=url)
        
        except ConnectionError:
            # Create error log entry
            url_error = pd.DataFrame(data={
                'name': source_name, 
                'url': url, 
                'timestamp': dt.datetime.now(), 
                'status': 'ConnectionError' }, index=[0])
            
            # Update main error log, create if does not exist 
            if os.path.isfile(log_filepath):
                url_error.to_csv(log_filepath, mode='a', header=False, index=False)            
            else:
                url_error.to_csv(log_filepath, mode='w', header=True, index=False)
            
            print(f'''Error processing {source_name} at {url} - see log for details.''')
            return None
        
        else:
            # Check status code is acceptable
            if requests.get(url=url).status_code not in accepted_codes:
                # Create error log entry
                url_error = pd.DataFrame(data={
                    'name': source_name, 
                    'url': url, 
                    'timestamp': dt.datetime.now(), 
                    'status': requests.get(url).status_code }, index=[0])
                
                # Update main error log, create if does not exist 
                if os.path.isfile(log_filepath):
                    url_error.to_csv(log_filepath, mode='a', header=False, index=False)            
                else:
                    url_error.to_csv(log_filepath, mode='w', header=True, index=False)
            
                print(f'''Error processing {source_name} at {url} - see log for details.''')
                return None

            else:
                return url

    def get_urls(self):
        sources = pd.read_csv("sources.csv")
        sources = sources[sources["Processor"] == self.type]
        
        for n, row in sources.iterrows():
            # Error checks
            test_link = self.source_test(
                source_name=row["Name"],
                url=row["Source URL"],
                log_filepath="error_log.csv")

            # URL assignment             
            if test_link is not None:
                self.urls[row["Name"]] = row["Source URL"]

    def get_datasets(self, owner, url, fname):
        print(owner, url, fname)

    def process(self):
        self.get_urls()

        for name, url in self.urls.items():
            self.get_datasets(name, url, os.path.join("data", self.type, name + ".csv"))


if __name__ == '__main__':
    for typ in [ 'tst', 'ckan', 'sparkql', 'dcat', 'arcgis', 'usmart']:
        tst = Processor(type=typ)
        tst.process()
        print()
