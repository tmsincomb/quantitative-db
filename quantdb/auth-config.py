{'config-search-paths': ['{:user-config-path}/quantdb/config.yaml',],
 'auth-variables': {
     # db
     'db-user': {
         'default': 'quantdb-user',
         'environment-variables': 'QUANTDB_DB_USER'},
     'db-host': {
         'default': 'localhost',
         'environment-variables': 'QUANTDB_DB_HOST'},
     'db-port': {
         'default': 5432,
         'environment-variables': 'QUANTDB_DB_PORT'},
     'db-database': {
         # we don't set a default here to prevent
         # accidental operations on a default db
         'default': None,
         'environment-variables': 'QUANTDB_DB_DATABASE QUANTDB_DATABASE',},
 }}
