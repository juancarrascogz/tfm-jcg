from tableausdk import *
from tableausdk.HyperExtract import *
import tableauserverclient as TSC

user='' # username Tableau
password='' # password Tableau
tableau_auth = TSC.TableauAuth(user, password)
server = TSC.Server('http://10.1.0.1')
server.version = '3.6'

with server.auth.sign_in(tableau_auth):
        print('connection made')
        print(server.version)
        server.workbooks.refresh(workbook_id='') # workbook_id Tableau
server.auth.sign_out()
print('connection closed')