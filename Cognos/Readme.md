# Cognos Windows Installation

Requirements: 
- IBM Runtime Environment, Java Technology Edition 8.0.1
- Oracle Java SDK/JRE/JDK

[Windows Single Server installation guide](https://www.ibm.com/support/knowledgecenter/en/SSEP7J_10.2.2/com.ibm.swg.ba.cognos.cbi.doc/pathway_install.html)

[Installation and Configuration guide](https://www.ibm.com/support/knowledgecenter/en/SSEP7J_10.2.2/com.ibm.swg.ba.cognos.cbi.doc/pathway_install.html#pageTop)

[Cognos Installation Guide for Windows](https://www.ibm.com/support/knowledgecenter/SSEP7J_11.0.0/com.ibm.swg.ba.cognos.inst_cr_winux.doc/inst_cr_winux.pdf)

[Cognos Installation Guide for Windows (new guide)](https://www.ibm.com/support/pages/how-install-ibm-cognos-analytics-tools-111x-framework-manager-cube-designer-and-dynamic-query-analyzer)

[Cognos Installation Guide for Windows (new guide 2)](https://www.ibm.com/support/pages/how-install-cognos-analytics-111x)

## Critical configuration actions to take first

### Ensure that JDBC drivers are in the correct location
For the IBM Cognos Analytics 11.0.x release, the JDBC drivers must be copied to theinstall_location\drivers directory.The use of install_location\webapps\p2pd\WEB-INF\lib for JDBC drivers is not supported.

### Replace the JSQL driver for Microsoft SQL Server with the Microsoft JDBC driver
Starting with IBM Cognos Analytics version 11.0.5, the JSQL driver for Microsoft SQL Server has beenreplaced with the Microsoft JDBC driver. You must download and place the required JAR file in theinstall_location\drivers directory. For more information, see Set up for a Microsoft SQL Servercontent store.

### Specify the Configuration Group property
If you used the Custom installation to install IBM Cognos Analytics, open IBM Cognos Configuration andset the Configuration Group property. For more information, see Managing the Configuration Group.

### Enable or disable web-based modeling
By default, JDBC data source connections that were created in IBM Cognos Administration are notexposed in the Manage > Data servers administration interface for use in data modules. If you want touse your existing (upgraded) data source connections to create data modules, you must enable web-based modeling on those connections.Some data sources are inappropriate to use as sources for creating data modules. In this case, you canprohibit the use of web-based modeling on the data source connections.To enable or disable web-based modeling for your data source connections, perform the following steps:
1. In IBM Cognos Analytics, go to Manage > Administration console.
2. In IBM Cognos Administration, on the Configuration tab, select Data source connections.3. Locate the data source, and click its Set properties action.4. On the Connection tab, select or clear the Allow web-based modeling check box.
3. Locate the data source, and click its Set properties action
4. On the Connection tab, select or clear the Allow web-based modeling check box

## Java requirements

To support the cryptographic services in IBM Cognos Analytics, you may be required to update yourversion of Java or set a JAVA_HOME environment variable. Depending on your security policyrequirements, you may also have to install the unrestricted Java Cryptography Extension (JCE) policy file.You can use an existing Java Runtime Environment (JRE) or the JRE that is provided with IBM CognosAnalytics.

### Cryptographic standards

IBM Cognos® cryptographic services use a specific jar (Java Archive) file, named bcprov-jdknn-nnn.jar, that must be in your Java Runtime Environment. This file provides additional encryption anddecryption routines that are not supplied as part of a default JVM (Java Virtual Machine) installation. Toensure security, the encryption file must be loaded by the JVM by using the Java extensions directory.

Note: For 11.0.2 and higher versions, the jar file is named bcprovpkix-XXX.jar
1. Go to the install_location/jre/lib/ext directory.
2. Copy the bcprov-jdk14-145.jarfile to your $JAVA_HOME/lib/ext directory.

### JAVA_HOME
Set a JAVA_HOME environment variable if you want to use your own Java.
Ensure that the JRE version is supported by IBM Cognos products.
On Microsoft Windows operating systems, if you do not have a JAVA_HOME variable, the JRE files that areprovided with the installation are used.To verify that your JRE is supported, see the [IBM Software Product Compatibility Reports page](www.ibm.com/support/docview.wss?uid=swg27047186).

## Review the default port settings
![image](/screenshots/ports.PNG)


## Installation of Cognos Analytics

[Official documentation, with images for steps](https://www.ibm.com/support/pages/how-install-ibm-cognos-analytics-tools-111x-framework-manager-cube-designer-and-dynamic-query-analyzer)


When opening the installer, you should be greeted by the following screen:

![image](/screenshots/cognos_install_1)


Then you will be requested to choose a file:

![image](/screenshots/cognos_install_2.PNG)


This refers to one of the 2 archives the installer came with. The 2 archives are a server and a client. The first tool we're installing is the base Cognos Analytics which is located in the ca_server_win archive:
![image](/screenshots/cognos_install_3.PNG)


Continue, then make sure you choose IBM Cognos Analytics

![image](/screenshots/cognos_install_4.PNG)


On this step, make sure you choose the desired location for the installation. If you attempted before to install it, this would've left a residue of folders behind. Trying to install to the same location will give you an error saying: Install location not empty, in this case, go to the folder in which you are trying to install and just delete everything inside. Continuing might give you a prompt saying that the destination folder does not exist, in which case it would ask you if it's ok to create one. Click yes.

![image](/screenshots/cognos_install_5.PNG)


Continue with the easy install:

![image](/screenshots/cognos_install_6.PNG)


Chose an admin ID and password for logging into the future installation of Cognos Analytics:

![image](/screenshots/cognos_install_7.PNG)


This last screen will confirm the installed product, user ID, location and size of the install, if everything looks good, hit install and wait:

![image](/screenshots/cognos_install_8.PNG)

## Installation of Cognos Tools

When prompted to choose a file, choose ca_client_MP instead of the ca_server that was previously used.
Keep hitting next until you get prompted to choose which tool you want installed. Simply check the box and continue. The tools installation process is a lot faster and more straight-forward than Cognos Analytics.


# Issues

[Issue affecting start-up of Cognos Configuration](https://www.ibm.com/support/pages/ibm-cognos-products-cnfg-has-stoped-working-error-cognos-analytics)
