# Apache License 2.0 https://github.com/datastax/dsbulk/blob/1.x/LICENSE.txt
# forked from on https://github.com/TrivadisPF/dockerfiles/blob/master/dse-dsbulk/Dockerfile

FROM dsbulk:1.8.0-r6

USER root

COPY --chown=dsbulk:dsbulk target/dsbulk-workflow-count-1.8.0.jar /dsbulk/lib/dsbulk-workflow-count-1.8.0.jar

USER dsbulk

# Run
#ENTRYPOINT [ "/dsbulk/bin/dsbulk" ]