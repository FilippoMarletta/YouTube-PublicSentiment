
FROM apache/spark:3.4.2

USER root

RUN pip install pyspark elasticsearch transformers torch google-cloud-language numpy 

WORKDIR /opt/youtubepublicsentiment

CMD ["/bin/bash"]