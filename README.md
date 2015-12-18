Usage:

vertx run com.alertme.test.BlueGreenProxy --instances 2 -cp classes/main -Dprimary.host=http://new.scs.url:8090 -Dsecondary.host=http://old.scs.url:8090

port is configurable by -Dport=8090 (which is default)