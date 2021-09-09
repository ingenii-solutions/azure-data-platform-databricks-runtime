.PHONY: build \
clean-all clean-lint clean-package clean-setup clean-tests clean-qa \
lint lint-convert \
test setup qa

-include .env

$(eval REGISTRY=$(shell grep '* Registry:' README.md | awk -F ':' '{print $$2}' | sed 's/ //g'))
$(eval REPOSITORY=$(shell grep '* Repository:' README.md | awk -F ':' '{print $$2}' | sed 's/ //g'))
$(eval VERSION=$(shell grep '* Current Version:' README.md | awk -F ':' '{print $$2}' | sed 's/ //g'))

# Package

update-pip:
	python -m pip install --upgrade pip

build-package:
	python setup.py bdist_wheel

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
	flake8 --tee --output-file flake8_report.txt

lint-convert:
	flake8_junit flake8_report.txt flake8_report_junit.xml

test:
	make clean-tests
	pytest ./tests --junitxml=pytest_report_junit.xml

qa:
	make lint
	make test

# Docker

build-container:
	docker build \
	--build-arg PACKAGE_VERSION=$(VERSION) \
	--build-arg DBT_SPARK_VERSION=$(shell cat requirements.txt | grep "dbt==" | awk -F'==' '{ print $$2 }') \
	-t $(REGISTRY)/$(REPOSITORY):$(VERSION) .

push-container:
	docker push $(REGISTRY)/$(REPOSITORY):$(VERSION)

# Overall

build: build-package build-container

push: push-container

build-and-push: build push

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
	cp ../azure-data-platform-data-engineering/dist/ingenii_data_engineering-0.2.0-py3-none-any.whl packages/
