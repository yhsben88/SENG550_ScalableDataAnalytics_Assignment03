#!/bin/bash

# Print the admin username and password
docker compose exec airflow cat /opt/airflow/simple_auth_manager_passwords.json.generated
