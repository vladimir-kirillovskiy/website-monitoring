# Website monitoring with Python, Kafka and PostgreSQL
Implementation of the system that monitors websites over network. Application works with local setup as well as with Aiven Kafka/PostgreSQL.
 
The application can monitor one or more websites from config.json. Additionaly regex string can be added to check for the match. Check interval can be set for each website. Producer checks the websites from config.json and sends them to topic. Consumer picks messages from topic and saves them into database.

Database table can be created with the create_table.sql script

## Instalation
### Arch linux
Install kafka - https://wiki.archlinux.org/index.php/Apache_Kafka

``` yay -S kafka ```

start kafka, which will also run zookeeper@kafka.service as well

``` sudo systemctl start kafka.service ```

Install PostgreSQL - https://wiki.archlinux.org/index.php/PostgreSQL

Install python libraries

``` pip install -r requirements.txt ```
