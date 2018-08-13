FROM base_lib

WORKDIR /

ADD target/groovy-cluster-worker-example-1.0-SNAPSHOT.jar main.jar
ADD target/lib lib

ENV DB_HOST localhost
ENV DB_PORT 5432
ENV DB_USER_NAME user
ENV DB_PASSWORD password

CMD java -cp main.jar:lib/* demo.Main

