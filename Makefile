.PHONY: build \
clean-all clean-lint clean-package clean-setup clean-tests clean-qa \
lint lint-convert \
test setup qa

-include .env

$(eval REGISTRY=$(shell grep '* Registry:' README.md | awk -F ':' '{print $$2}' | sed 's/ //g'))
$(eval REPOSITORY=$(shell grep '* Repository:' README.md | awk -F ':' '{print $$2}' | sed 's/ //g'))
$(eval VERSION=$(shell grep '* Version:' README.md | awk -F ':' '{print $$2}' | sed 's/ //g'))

$(eval BASE_OS_REPOSITORY=$(shell grep '* Base OS Repository:' README.md | awk -F ':' '{print $$2}' | sed 's/ //g'))
$(eval BASE_OS_VERSION=$(shell grep '* Base OS Version:' README.md | awk -F ':' '{print $$2}' | sed 's/ //g'))

$(eval BASE_PYTHON_REPOSITORY=$(shell grep '* Base Python Repository:' README.md | awk -F ':' '{print $$2}' | sed 's/ //g'))
$(eval BASE_PYTHON_VERSION=$(shell grep '* Base Python Version:' README.md | awk -F ':' '{print $$2}' | sed 's/ //g'))

PROJECT_ROOT := $(realpath .)

setup:
	cp .env-dist .env

# Package

update-pip:
	python -m pip install --upgrade pip

build-package:
	python setup.py bdist_wheel

# Lint and Unit tests

clean-all:
	make clean-lint
	make clean-package
	make clean-tests

clean-qa:
	make clean-lint
	make clean-tests

clean-package:
	@rm -rf ./build ./dist ./*.egg-info ./.eggs

clean-lint:
	@rm -rf ./flake8_report.txt ./flake8_report_junit.xml

clean-setup:
	@rm .env

clean-tests:
	@rm -rf ./.pytest_cache **/__pycache__ ./pytest_report_junit.xml

lint:
	make clean-lint
	flake8 --tee --output-file flake8_report.txt ingenii_databricks

lint-convert:
	flake8_junit flake8_report.txt flake8_report_junit.xml

test:
	make clean-tests
	pytest ./unit_tests --junitxml=pytest_report_junit.xml

qa:
	make lint
	make test

# Docker

build-container-base-os:
	docker build \
		-f Dockerfile.os \
		-t $(REGISTRY)/$(BASE_OS_REPOSITORY):$(BASE_OS_VERSION) .

push-container-base-os:
	docker push $(REGISTRY)/$(BASE_OS_REPOSITORY):$(BASE_OS_VERSION)

build-container-base-python:
	docker build \
		-f Dockerfile.python \
		--build-arg REGISTRY=$(REGISTRY) \
		--build-arg REPOSITORY=$(BASE_OS_REPOSITORY) \
		--build-arg VERSION=$(BASE_OS_VERSION) \
		-t $(REGISTRY)/$(BASE_PYTHON_REPOSITORY):$(BASE_PYTHON_VERSION) .

push-container-base-python:
	docker push $(REGISTRY)/$(BASE_PYTHON_REPOSITORY):$(BASE_PYTHON_VERSION)

build-container:
	docker build \
		--build-arg PACKAGE_VERSION=$(VERSION) \
		-t $(REGISTRY)/$(REPOSITORY):$(VERSION) .

push-container:
	docker push $(REGISTRY)/$(REPOSITORY):$(VERSION)

build: build-package build-container

push: push-container

build-and-push: build push

# Infrastructure

PULUMI_FOLDER := ${PROJECT_ROOT}/integration_tests/infrastructure
PULUMI_ORGANIZATION	:= ingenii
PULUMI_STACK := ${PULUMI_ORGANIZATION}/databricks-runtime-testing
PULUMI_PARALLELISM	:= 2

# Authenticate with user permissions + `az login`
az-login:
	az account set --subscription=${ARM_SUBSCRIPTION_ID}

pulumi_init:
	pulumi --cwd $(PULUMI_FOLDER) stack select $(PULUMI_STACK) --create --color always --non-interactive

pulumi_preview:
	pulumi --cwd $(PULUMI_FOLDER) preview --stack $(PULUMI_STACK) --color always --diff --non-interactive

pulumi_refresh:
	pulumi --cwd $(PULUMI_FOLDER) refresh --stack $(PULUMI_STACK) --color always --diff --skip-preview --non-interactive --yes

pulumi_apply:
	pulumi --cwd $(PULUMI_FOLDER) up --stack $(PULUMI_STACK) --parallel ${PULUMI_PARALLELISM} --color always --diff --skip-preview --non-interactive --yes

pulumi_destroy:
	pulumi destroy --cwd $(PULUMI_FOLDER) --stack $(PULUMI_STACK) --parallel ${PULUMI_PARALLELISM} --color always
	pulumi stack rm --cwd $(PULUMI_FOLDER) --stack $(PULUMI_STACK) --non-interactive --yes

# Integration tests

data_sync:
	./integration_tests/scripts/data_sync.sh

notebook_sync:
	./integration_tests/scripts/notebook_sync.sh

run_integration_tests:
	@$(eval DATABRICKS_AAD_TOKEN=$(shell az account get-access-token --subscription ${ARM_SUBSCRIPTION_ID} --resource 2ff814a6-3304-4ab8-85cb-cd0e6f879c1d | jq '.accessToken' --raw-output))

	@DATABRICKS_AAD_TOKEN=${DATABRICKS_AAD_TOKEN} python ./integration_tests/scripts/submit_job.py

# Other

ubuntu-setup:
	python3 -m venv venv
	source venv/bin/activate
	python -m pip install -U pip
	pip install -r requirements.txt
	pip install databricks-cli

get-aad-token:
	export DATABRICKS_AAD_TOKEN=$(az account get-access-token --tenant 20b68445-3d97-4c4f-981c-fe8ffc7ffaa7 | jq .accessToken --raw-output)
	databricks configure --aad-token

upload-notebooks:
	find notebooks -iname '*.py' | awk -F'/' '{print $$2}' | awk -F'.' '{print $$1}' | xargs -I {} databricks workspace import -l PYTHON notebooks/{}.py /Shared/Ingenii\ Engineering/{}

get-package:
	cp ../azure-data-platform-data-engineering/dist/ingenii_data_engineering-0.2.1-py3-none-any.whl packages/
