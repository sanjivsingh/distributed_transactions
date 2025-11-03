
# start mysql

sudo /usr/local/mysql/bin/mysqld --user=_mysql --basedir=/usr/local/mysql --datadir=/usr/local/mysql/data --plugin-dir=/usr/local/mysql/lib/plugin --log-error=/usr/local/mysql/data/mysqld.local.err --pid-file=/usr/local/mysql/data/mysqld.local.pid


/usr/local/mysql/etc/my.cnf

[mysqld]
mysql_native_password=ON


cd /usr/local/mysql/bin/
sudo mysqld --user=mysql


# installation directory

/usr/local/mysql/bin

# add to path
echo "export PATH=$PATH:/usr/local/mysql/bin/" >> ~/.bash_profile
source ~/.bash_profile

# CLI connect to mysql
mysql -u root -prootadmin

# troubleshoot

## RuntimeError: 'cryptography' package is required for sha256_password or caching_sha2_password auth methods

even after **pip install cryptography** , it is giving error 
Run these commands in your MySQL shell (or via a client like MySQL Workbench):

```
ALTER USER 'root'@'localhost' IDENTIFIED WITH sha256_password BY 'rootadmin';
FLUSH PRIVILEGES;
```

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
