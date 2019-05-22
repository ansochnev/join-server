# join_server

Add this repo to your /etc/apt/sources.list:
```
echo "deb https://dl.bintray.com/ansochnev/otus-cpp/ homework main" >> /etc/apt/sources.list
```

Get the bintray public key:
```
curl https://bintray.com/user/downloadSubjectPublicKey?username=bintray > bintray-public-key.asc
```

Add the key to apt:
```
apt-key add bintray-public-key.asc
```

Install:
```
apt update
apt-get install join_server
```

Run
```
join_server <port>
```
