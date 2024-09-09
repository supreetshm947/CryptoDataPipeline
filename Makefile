# Load environment variables from .env file
include .env

# Export environment variables
export $(shell sed 's/=.*//' .env)

# Ensure example DAGs are not loaded
export AIRFLOW__CORE__LOAD_EXAMPLES=False

# Specify the custom DAGs folder
export AIRFLOW__CORE__DAGS_FOLDER=./dags/

# Set the PYTHONPATH to the project root
export PYTHONPATH=.

# Virtual environment path
VENV_PATH=.venv/bin/activate

# Targets for Airflow setup
init_db:
	. $(VENV_PATH) && airflow db upgrade
	. $(VENV_PATH) && airflow db reset --yes
	. $(VENV_PATH) && airflow db init

create_user:
	. $(VENV_PATH) && airflow users create \
		--username $(AIRFLOW_USER_NAME) \
		--firstname $(AIRFLOW_USER_FIRSTNAME) \
		--lastname $(AIRFLOW_USER_LASTNAME) \
		--role Admin \
		--email $(AIRFLOW_USER_EMAIL) \
		--password $(AIRFLOW_USER_PASSWORD)

setup: init_db create_user
	@echo "Airflow setup complete."

# Targets for starting Airflow
start_webserver:
	. $(VENV_PATH) && airflow webserver --port 8080

start_scheduler:
	. $(VENV_PATH) && airflow scheduler

start: start_webserver start_scheduler
	@echo "Airflow is running."

# Clean up (if needed)
clean:
	@echo "Stopping Airflow services..."
	# Add any cleanup commands if needed
