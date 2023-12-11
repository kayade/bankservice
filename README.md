Bankservice

This is a microservice that manages and processes all email related operations.

This microservice is built with Java

HOW TO COMPILE PACKAGE

 - the "lib" & "src" folder needs to be in the directory where the command is called
 - compile package [windows] [javac -cp ".;lib/*;" src/bankservice/*.java -d .]
 - compile package [others] [javac -cp ".:lib/*:" src/bankservice/*.java -d .]
 

HOW TO BUILD A EXECUTABLE JAR FILE

 - the MANIFEST file (manifest.mf) needs to be in the directory where the command is called 
 - the "lib" folder and the generated class folder needs to be in the directory where the command is called
 - to create executable jar file [jar -cvfm bankservice.jar MANIFEST.MF bankservice/*.class]


HOW TO RUN THE EXECUTABLE JAR FILE (Bankservice)

 - the "mservices.jks" file needs to be in the directory where the command is called
 - the "lib" folder needs to be in the directory where the command is called
 - [java -jar bankservice.jar]

