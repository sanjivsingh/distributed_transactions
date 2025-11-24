brew install mysql@8.0

```
==> /usr/local/Cellar/mysql@8.0/8.0.44_2/bin/mysqld --initialize-insecure --user=sanjivsingh --basedir=/usr/local/Cellar/mysql@8.0/8.0.44_2 --datadir=/usr/local/var/mysql --tmpdir=/tmp
==> Caveats

We've installed your MySQL database without a root password. To secure it run:
    mysql_secure_installation

MySQL is configured to only allow connections from localhost by default

To connect run:
    mysql -u root

mysql@8.0 is keg-only, which means it was not symlinked into /usr/local,
because this is an alternate version of another formula.

If you need to have mysql@8.0 first in your PATH, run:
  echo 'export PATH="/usr/local/opt/mysql@8.0/bin:$PATH"' >> ~/.zshrc

For compilers to find mysql@8.0 you may need to set:
  export LDFLAGS="-L/usr/local/opt/mysql@8.0/lib"
  export CPPFLAGS="-I/usr/local/opt/mysql@8.0/include"

  echo 'export LDFLAGS="-L/usr/local/opt/mysql@8.0/lib"' >> ~/.zshrc
  echo 'export CPPFLAGS="-I/usr/local/opt/mysql@8.0/include"' >> ~/.zshrc
```


# To start mysql@8.0 now and restart at login:
```
  brew services start mysql@8.0
```

# change root password
```
mysql_secure_installation
```

# CLI connect to mysql root
```
mysql -u root -prootadmin
```

# create new user

```
CREATE USER 'prototype'@'localhost' IDENTIFIED WITH mysql_native_password BY 'prototype_password';
GRANT CREATE ON *.* TO 'prototype'@'localhost';
FLUSH PRIVILEGES;

GRANT ALL PRIVILEGES ON *.* TO 'prototype'@'localhost';
FLUSH PRIVILEGES;


```

# start mysql

sudo /usr/local/mysql/bin/mysqld --user=_mysql --basedir=/usr/local/mysql --datadir=/usr/local/mysql/data --plugin-dir=/usr/local/mysql/lib/plugin --log-error=/usr/local/mysql/data/mysqld.local.err --pid-file=/usr/local/mysql/data/mysqld.local.pid


cd /usr/local/mysql/bin/
sudo mysqld --user=mysql


# installation directory

/usr/local/mysql/bin

# add to path
echo "export PATH=$PATH:/usr/local/mysql/bin/" >> ~/.bash_profile
source ~/.bash_profile

# CLI connect to mysql
mysql -u root -prootadmin


# check user PRIVILEGES 
```
SELECT user, host, plugin FROM mysql.user
```

# check version 
```
mysql --version          
mysql  Ver 9.4.0 for macos15 on x86_64 (MySQL Community Server - GPL)
```


# troubleshoot

## RuntimeError: 'cryptography' package is required for sha256_password or caching_sha2_password auth methods

even after **pip install cryptography** , it is giving error 
Run these commands in your MySQL shell (or via a client like MySQL Workbench):

```
ALTER USER 'root'@'localhost' IDENTIFIED WITH sha256_password BY 'rootadmin';
FLUSH PRIVILEGES;
```

# setup user for apps

```
CREATE USER 'proto'@'localhost' IDENTIFIED WITH sha256_password BY 'proto_password';
FLUSH PRIVILEGES;






CREATE USER 'tempuser'@'localhost' IDENTIFIED WITH caching_sha2_password BY 'tempuser';
FLUSH PRIVILEGES;
```

# check certificate name
SQL> select * from performance_schema.global_variables
     where variable_name like 'ssl_ca';
+---------------+----------------+
| VARIABLE_NAME | VARIABLE_VALUE |
+---------------+----------------+
| ssl_ca        | ca.pem         |
+---------------+----------------+





cd /usr/local/mysql
sudo openssl req -newkey rsa:2048 -days 365 -nodes -keyout prototype-key.pem -out prototype-req.pem



sudo openssl req -newkey rsa:2048 -days 365 -nodes -keyout prototype-key.pem -out prototype-req.pem

Password:
Warning: Ignoring -days without -x509; not generating a certificate
.+.+..+.+.....+....+...........+....+...+...+..+............+...+.............+......+++++++++++++++++++++++++++++++++++++++*..+.........+..+++++++++++++++++++++++++++++++++++++++*.....+.+...........+....+...........+...+.....................+.+..+......................+........+...+....+...+..+.+........+.+.........+.....+......+.............+..+................++++++
....+...........+.............+++++++++++++++++++++++++++++++++++++++*...+..........+..+.............+...+.....+.+......+..+...+...+++++++++++++++++++++++++++++++++++++++*.+..+.+..+....+.....+.+..+......+.+.....+...............+....+......+........+......+.+........+...............+............+.+.....+.......+........+...+...............+.........+...+....+.........+.........+.....+.+.........+.....+.+.....................+.................................+.................+....+..+....+...+.................+.+........+.+.....+....+..+....+...........+....+...+......+..+............+.+......+.....+...+...+..........+..+.......+........+.+.....+.......+.....+.......+..+..........+.........+.....+.......+..+.+.........+..+...+.+...............+......+...+..+.........+......+..........+.....+......+....+...........+...+.......+...+..............+...+...+.++++++
-----
You are about to be asked to enter information that will be incorporated
into your certificate request.
What you are about to enter is what is called a Distinguished Name or a DN.
There are quite a few fields but you can leave some blank
For some fields there will be a default value,
If you enter '.', the field will be left blank.
-----
Country Name (2 letter code) [AU]:AU
State or Province Name (full name) [Some-State]:UP
Locality Name (eg, city) []:RBL
Organization Name (eg, company) [Internet Widgits Pty Ltd]:Impetus
Organizational Unit Name (eg, section) []:IT
Common Name (e.g. server FQDN or YOUR name) []:
Email Address []:prototype@google.com

Please enter the following 'extra' attributes
to be sent with your certificate request
A challenge password []:prototype
An optional company name []:prototype


sudo openssl x509 -req -in prototype-req.pem -days 365 -CA /usr/local/mysql/data/ca.pem \
     -CAkey /usr/local/mysql/data/ca-key.pem -set_serial 01 -out prototype-cert.pem

(.venv) sanjivsingh@IMUL-ML0406 mysql % sudo openssl x509 -req -in prototype-req.pem -days 365 -CA /usr/local/mysql/data/ca.pem \
     -CAkey /usr/local/mysql/data/ca-key.pem -set_serial 01 -out prototype-cert.pem
Certificate request self-signature ok
subject=C=AU, ST=UP, L=RBL, O=Impetus, OU=IT, emailAddress=prototype@google.com






sudo openssl verify -CAfile /usr/local/mysql/data/ca.pem /usr/local/mysql/data/server-cert.pem \
  prototype-cert.pem


(.venv) sanjivsingh@IMUL-ML0406 mysql % sudo openssl verify -CAfile /usr/local/mysql/data/ca.pem /usr/local/mysql/data/server-cert.pem \
  prototype-cert.pem
/usr/local/mysql/data/server-cert.pem: OK
prototype-cert.pem: OK


MySQL User Creation
We need to create a MySQL user that will use the certificate. By default, with the loaded password policy, we also need to provide a password:

mysql> CREATE USER prototype IDENTIFIED BY 'prototype_pass' REQUIRE        SUBJECT '/C=AU/ST=UP/L=RBL/O=Impetus/OU=IT/CN=prototype/emailAddress=prototype@google.com';
Query OK, 0 rows affected (0.080 sec)

mysql> FLUSH PRIVILEGES;
Query OK, 0 rows affected, 1 warning (0.003 sec)



(.venv) sanjivsingh@IMUL-ML0406 mysql % sudo mysql --user prototype -p prototype_pass --host localhost --port 3306 --ssl-cert prototype-cert.pem --ssl-key prototype-key.pem 
Password:
Enter password: 
ERROR 1045 (28000): Access denied for user 'prototype'@'localhost' (using password: YES)