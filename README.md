# cca
populate_db.py takes all files under cca/database/ and uploads to a mongodb server  
Attention it will drop a table called cca on the server before repopulating it  

fix lib/database.py and add your server ip  
create a folder named database and add your files there

only cvs or dbf are available now  


## instructions

on your workplace:  
```
python3 -m venv cca_env  
cd cca_env
source bin/activate
mkdir src
cd src
git clone https://github.com/arturrocha/cca
cd cca
pip install -r requirements.txt
pytest
```
