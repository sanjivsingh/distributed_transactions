# install redis

brew install redis



# To start redis now and restart at login:
  brew services start redis

# Or, if you don't want/need a background service you can just run:
  /usr/local/opt/redis/bin/redis-server /usr/local/etc/redis.conf

