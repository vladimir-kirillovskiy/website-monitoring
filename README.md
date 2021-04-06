# Website monitoring with Python, Kafka and PostgreSQL
Implementation of the system that monitors websites over network. Application works with local setup as well as with Aiven Kafka/PostgreSQL.
 
The application can monitor one or more websites from config.json. Additionaly regex string can be added to check for the match. Check interval can be set for each website. Producer checks the websites from config.json and sends them to topic. Consumer picks messages from topic and saves them into database.

Database table can be created with the create_table.sql script

