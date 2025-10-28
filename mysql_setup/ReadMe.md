# installation directory

/usr/local/mysql/bin

# add to path
echo "export PATH=$PATH:/usr/local/mysql/bin/" >> ~/.bash_profile
source ~/.bash_profile

# connect to mysql

mysql -u root -p rootadmin


# setup table 

mysql> 

create database conversion_db;

use conversion_db;

CREATE TABLE conversion_db.conversion_request (
    id bigint,
    filename varchar(50),
    input_format varchar(50),
    output_format varchar(50),
    upload_path varchar(50), 
    converted_path varchar(50),
    status varchar(50),
    details Text,
    file_hash varchar(50)
)
