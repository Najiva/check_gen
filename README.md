# check_gen
Generates CHECK constraints for set of non-null columns for each state value of the database table record.

# Dependencies:

pip install psycopg2

# Setup

In the file 'icheck.py' set the database connection details and non-null columns restrictions based on the states and desired checking (see example). Python script will try to connect to the postgres database and apply generated CHECK constraints to the database. Each constraints name ends with "_icheck".

# Run

python icheck.py
