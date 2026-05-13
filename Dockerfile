FROM airbyte/python-connector-base:2.0.0

WORKDIR /airbyte/integration_code

COPY . ./

RUN pip install --no-cache-dir .

# Required by the Airbyte v2 workload runner
ENV AIRBYTE_ENTRYPOINT="python -m source_customer_thermometer"

ENTRYPOINT ["python", "-m", "source_customer_thermometer"]
