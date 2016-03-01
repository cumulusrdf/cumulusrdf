FROM maven:3.3-jdk-8
RUN useradd cumulusrdf -d /usr/src/app -m -s /bin/bash
ADD . /usr/src/app/
RUN chown -R cumulusrdf:cumulusrdf /usr/src/app

USER cumulusrdf
WORKDIR /usr/src/app
RUN mvn -DskipTests install -Pcassandra2x-cql-full-tp-index 
WORKDIR cumulusrdf-web-module/

EXPOSE 9090
ENTRYPOINT ["mvn"]
CMD ["farsandra:start", "cargo:run","-Pcassandra2x-cql-full-tp-index"]
