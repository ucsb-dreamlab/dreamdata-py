import dreamdata
from datetime import date, timedelta

# DREAM Lab repository API endporint
repo = "https://ucsb-dreamlab-index.westus2.cloudapp.azure.com"

# client authenticates with tls certificate
cert = "/Users/serickson-local/sericksonucsb.edu.crt"
key  = "/Users/serickson-local/sericksonucsb.edu.key"

name = 'nytimes'
start = date(1910,1,1)
end = date(1910,1,31)

with dreamdata.Client(repo, client_key=key, client_cert=cert) as client:
    d = start
    while d <= end:
        for record in client.pqnews_namedate('nytimes', d):
            print(record.record_title)
        d = d + timedelta(days=1)