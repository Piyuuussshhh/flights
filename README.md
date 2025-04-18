# Instructions
1. Create a python virtual environment (optional) and activate it.
    - `$ python -m venv venv`
    - `$ .\venv\Scripts\activate`
2. Install the required dependencies using the following command:
    - `pip install -r requirements.txt`
3. Run all the cells in `cleaning.ipynb`. This writes a cleaned `flights.csv` to the directory above the current working directory, under a new folder named `datasets`.
4. Create a postgres database.
5. Create a .env file in the current working directory and fill in the following fields:
    - DB_NAME=\<the name of your postgres database>
    - DB_USER=postgres
    - DB_PASS=\<your postgres password>
    - DB_HOST=localhost
    - DB_PORT=5432
6. Run `flights.db` and wait for insertion to complete.