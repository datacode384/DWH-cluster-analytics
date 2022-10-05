# Python virtual environment(venv) setup

- yum upgrade before setting up venv 
  ``` 
  yum update
  yum groupinstall -y "development tools"
  yum install -y zlib-devel bzip2-devel openssl-devel ncurses-devel sqlite-devel readline-devel tk-devel gdbm-devel db4-devel libpcap-devel xz-devel expat-devel
  yum install -y wget
  ```
- Upgrade python2.x in centos7 to pythin 3.x as shown below. (Upgraded to 3.7.1 in new_dev. same python version as in old_dev)
```
wget http://python.org/ftp/python/3.7.1/Python-3.7.1.tar.xz
tar xf Python-3.7.1.tar.xz
cd Python-3.7.1
./configure --prefix=/usr/local --enable-shared LDFLAGS="-Wl,-rpath /usr/local/lib"
make && make altinstall
strip /usr/local/lib/libpython3.7m.so.1.0
```
- Install pip
```
wget https://bootstrap.pypa.io/get-pip.py
python3.7 get-pip.py
```
- Use the upgraded python for creating python venv as shown below. create venv named venv37 in user directory
    ```
    /usr/local/bin/python3.7 -m venv R1_I5
    ```
- Activate venv
  ```
  source R1_I5/bin/activate
  ```
-  Install packages to venv with ```pip install packages```
 
 ```python -m site``` gives you the location of pip installed packages within venv 

Note: To Deactivate venv   ```deactivate```

Ref: [venv setup](https://danieleriksson.net/2017/02/08/how-to-install-latest-python-on-centos/) 

## Issues:
1. Yum lock  
```
ps -ef | grep pid
sudo kill -9 pid (or) pkill PackageKit
```
- https://www.thegeekdiary.com/yum-command-fails-with-another-app-is-currently-holding-the-yum-lock-in-centos-rhel-7/
      

