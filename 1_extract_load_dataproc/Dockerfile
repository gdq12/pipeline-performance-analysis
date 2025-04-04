FROM spark:3.5.1-scala2.12-java11-python3-ubuntu
# python version 3.8.10 being used

# for non root users so can pip install
# based on this post:
# https://stackoverflow.com/questions/71753536/how-to-deal-with-permission-errors-with-pip-inside-a-docker-compose-container
# https://stackoverflow.com/questions/62988075/what-is-a-clean-way-to-add-a-user-in-docker-with-sudo-priviledges
# https://mydeveloperplanet.com/2022/10/19/docker-files-and-volumes-permission-denied/

USER root
ARG USERNAME=ubuntu
RUN addgroup --group sparkgroup
RUN useradd -ms /bin/bash --uid 1000 ${USERNAME}
RUN usermod -aG sudo ${USERNAME}
USER ${USERNAME}

# install gcloud CLI
RUN curl -sSL https://sdk.cloud.google.com | bash
ENV PATH $PATH:/home/${USERNAME}/google-cloud-sdk/bin

# final working directory
WORKDIR /home/${USERNAME}
ENV PATH="$PATH:/home/${USERNAME}/.local/bin"
EXPOSE 8501

# pip install all other libraries
COPY --chown=${USERNAME}:sparkgroup requirements.txt /home/${USERNAME}/
RUN cd /home/${USERNAME} && \
    pip install --no-cache-dir -r requirements.txt

# install hadoop connector for pyspark - gcp
RUN wget https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar
# also done in terminal via
# gsutil cp gs://hadoop-lib/gcs/gcs-connector-hadoop3-2.2.5.jar /home/ubuntu/lib/gcs-connector-hadoop3-latest.jar

# install spark in directory that have writting access to
# https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/05-batch/setup/linux.md
RUN wget https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz && \
  tar xzfv spark-3.5.1-bin-hadoop3.tgz && \
  rm spark-3.5.1-bin-hadoop3.tgz
ENV PATH="${SPARK_HOME}/bin:${PATH}"

# env variables needed to be defined
ENV HOME=/home/${USERNAME}
# ENV HADOOP_COMMON_LIB_JARS_DIR=$HADOOP_CLASSPATH:$HOME/gcs-connector-hadoop3-latest.jar
ENV SPARK_HOME="${HOME}/spark-3.5.1-bin-hadoop3"

# command to run when container launched
CMD jupyter lab --ip=0.0.0.0 --port=8888 --no-browser --allow-root --NotebookApp.token=