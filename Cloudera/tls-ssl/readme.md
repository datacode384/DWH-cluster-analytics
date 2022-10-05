# TLS/SSL for Cloudera

[Official Documentation](https://docs.cloudera.com/documentation/enterprise/5-11-x/topics/how_to_configure_cm_tls.html#concept_gkg_xs3_lx)

## How to generate TLS certificates

On each cluster host:
```export JAVA_HOME=/usr/java/jdk1.8.0_162``` , you can check the version with ```javac -version```

```
sudo mkdir -p /opt/cloudera/security/pki
$JAVA_HOME/bin/keytool -genkeypair -alias $(hostname -f) -keyalg RSA -keystore /opt/cloudera/security/pki/$(hostname -f).jks -keysize 2048 -dname "CN=$(hostname -f),OU=Engineering,O=Cloudera,L=Palo Alto,ST=California,C=US" -ext san=dns:$(hostname -f)
```
```OU```, ```O```, ```L```, ```ST```, ```C``` values have to be changed to values for our environment.

```$JAVA_HOME/bin/keytool -certreq -alias $(hostname -f) -keystore /opt/cloudera/security/pki/$(hostname -f).jks -file /opt/cloudera/security/pki/$(hostname -f).csr -ext san=dns:$(hostname -f) -ext EKU=serverAuth,clientAuth```

Submit the CSR files (for example, cm01.example.com.csr) to your certificate authority to obtain a server certificate. After you have received the signed certificate, copy the signed certificate to the following location: ```/opt/cloudera/security/pki/$(hostname -f).pem```

Inspect the signed certificate to verify that both server and client authentication options are present, as well as the subject alternative name:

openssl x509 -in /opt/cloudera/security/pki/$(hostname -f).pem -noout -text

Look for output similar to the following to verify the server and client authentication options:
```
            X509v3 Extended Key Usage:
                TLS Web Server Authentication, TLS Web Client Authentication
```

Look for output similar to the following to validate the subject alternative name:
```
            X509v3 Subject Alternative Name:
                DNS:hostname.example.com
```

Copy the JDK cacerts file to jssecacerts as follows: ```sudo cp $JAVA_HOME/jre/lib/security/cacerts $JAVA_HOME/jre/lib/security/jssecacerts``` or ```/cacerts```. The default password is ```changeit```.

Import the root CA certificate into the JDK truststore: ```sudo $JAVA_HOME/bin/keytool -importcert -alias rootca -keystore $JAVA_HOME/jre/lib/security/jssecacerts -file /opt/cloudera/security/pki/rootca.pem```


Copy the root and intermediate CA certificates to ```/opt/cloudera/security/pki/rootca.pem``` and ```/opt/cloudera/security/pki/intca.pem``` on each host.

Copy the JDK cacerts file to jssecacerts as follows: ```sudo cp $JAVA_HOME/jre/lib/security/cacerts $JAVA_HOME/jre/lib/security/jssecacerts```

Import the root CA certificate into the JDK truststore: ```sudo $JAVA_HOME/bin/keytool -importcert -alias rootca -keystore $JAVA_HOME/jre/lib/security/jssecacerts -file /opt/cloudera/security/pki/rootca.pem```

Type ```yes``` when asked to trust the certificate

Append the intermediate CA certificate to the signed host certificate, and then import it into the keystore. Make sure that you use the append operator (```>>```) and not the overwrite operator (```>```): ```sudo cat /opt/cloudera/security/pki/intca.pem >> /opt/cloudera/security/pki/$(hostname -f).pem```



# TLS/SSL for Hue

[Official Documentation](https://docs.cloudera.com/documentation/enterprise/5-11-x/topics/cm_sg_ssl_hue.html#xd_583c10bfdbd326ba--6eed2fb8-14349d04bee--76bf)

