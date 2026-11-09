# Imagem do Airflow local com as dependências extras das coletas (openpyxl lê o XLSX do SP156).
ARG AIRFLOW_VERSION=2.10.5
FROM apache/airflow:${AIRFLOW_VERSION}
ARG AIRFLOW_VERSION
COPY requirements.txt /tmp/requirements.txt
# Fixar apache-airflow impede o pip de trocar a versão do Airflow ao resolver as dependências.
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /tmp/requirements.txt
