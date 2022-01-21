Included docker-compose file, so you can up spin up the airflow using docker with docker-compose file

additional settings : need to set up postgres db connection :
please use below settings to set up postgres connection:
settings : 
connection id : postgres_local
connection type : postgres
host: host.docker.internal
schema: default or postgres or your own schema
login : username
password : 
port : 5432 (default) if you have changed use the correct port
