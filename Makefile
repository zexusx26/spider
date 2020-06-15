#!/usr/bin/make
# Makefile readme (ru): <http://linux.yaroslavl.ru/docs/prog/gnu_make_3-79_russian_manual.html>
# Makefile readme (en): <https://www.gnu.org/software/make/manual/html_node/index.html#SEC_Contents>

dc_bin := $(shell command -v docker-compose 2> /dev/null)

SHELL = /bin/sh
RUN_APP_ARGS = --rm app

.PHONY : help build shell test lint start shutdown restart logs clean
.DEFAULT_GOAL : help

# This will output the help for each task. thanks to https://marmelab.com/blog/2016/02/29/auto-documented-makefile.html
help: ## Show this help
	@printf "\033[33m%s:\033[0m\n" 'Available commands'
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  \033[32m%-14s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

build: ## Build image
	$(dc_bin) build

shell: ## Start shell into app container
	$(dc_bin) run $(RUN_APP_ARGS) sh

lint: ## Run flake8 for work directory in app container
	$(dc_bin) run $(RUN_APP_ARGS) flake8 .

start: ## Create and start containers
	$(dc_bin) up -d

shutdown: ## Stop and remove containers, networks, images, and volumes
	$(dc_bin) down -t 5

restart: shutdown start ## Shutdown and start containers

logs: ## Show output from containers
	$(dc_bin) logs

test: ## Execute tests
	$(dc_bin) run $(RUN_APP_ARGS) pytest --disable-warnings
	$(dc_bin) run $(RUN_APP_ARGS) python3 ./spider/scrapper.py
