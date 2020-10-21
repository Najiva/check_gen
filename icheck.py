#!/usr/bin/python3

import psycopg2

# Example definition
# Lets say the states transiton is as follows:
#  Order: 
#    not_paid -> paid
# And we have to be make sure that not_paid orders have telehone contact number stored in the database in the column order.telephone and paid orders have an invoice generated.

data = {
    'database': {
        'name': "",
        'user': "",
        'password': "",
        'host': "",
        'port': "",
    },
    'table': 'order',
    'state_column': 'state',
    'allowed_states': [
        {
            'value': 'paid', 
            'not_null_columns': ['invoice_number']
        },
        {
            'value': 'not_paid', 
            'not_null_columns': ['telephone']
        },
    ]
}

# Validating that columns exist
def validate_structure():
    # TODO
    pass

# Generate single check constraint for each state
def generate_state_check_constraint(data):
    allowed_states = list(map(lambda x: x['value'], data['allowed_states']))
    check_name = '{}_{}_icheck'.format(
        data['table'],
        data['state_column'])
    constraint = 'ALTER TABLE {} ADD CONSTRAINT {} CHECK ({} IN {});'.format(
        data['table'], 
        check_name,
        data['state_column'],
        str(allowed_states).replace('[', '(').replace(']', ')')
        )
    return constraint, check_name

def apply_constraint(sql, check_name, cursor, connection):
    print("Applying constraint {}".format(check_name))
    cursor.execute(sql)
    connection.commit()

def drop_constraint(table_name, check_name, cursor, connection):
    print("Dropping constraing {} if exists.".format(check_name))
    cursor.execute('ALTER TABLE {} DROP CONSTRAINT IF EXISTS {};'.format(table_name, check_name))
    connection.commit()

def main():
    try:
        connection = psycopg2.connect(user = data['database']['user'],
                                    password = data['database']['password'],
                                    host = data['database']['host'],
                                    port = data['database']['port'],
                                    database = data['database']['name'])
        cursor = connection.cursor()
        validate_structure()
        # Create first constraint on values of the states column
        sql, check_name = generate_state_check_constraint(data)
        drop_constraint(data['table'], check_name, cursor, connection)
        apply_constraint(sql, check_name, cursor, connection)

        # Create non-null constraints of other columns based on state of the state column
        for state in data['allowed_states']:
            value = state['value']
            if 'not_null_columns' not in state or not state['not_null_columns']:
                continue
            columns = state['not_null_columns']
            check_name = '{}_{}_{}_icheck'.format(data['table'], data['state_column'], str(value))
            required_columns = ' or '.join(map(lambda x: str(x) + ' is null ', columns))
            sql = 'ALTER TABLE {} ADD CONSTRAINT {} CHECK ( CASE WHEN {} = {} AND ({}) THEN false ELSE true END);'.format(data['table'], check_name, data['state_column'], value, required_columns)
            print(sql)
            drop_constraint(data['table'], check_name, cursor, connection)
            apply_constraint(sql, check_name, cursor, connection)
 
    except (Exception, psycopg2.Error) as error :
        print ("Error while connecting to PostgreSQL", error)
    finally:
        #closing database connection.
        if(connection):
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")

if __name__ == "__main__":
   main()
