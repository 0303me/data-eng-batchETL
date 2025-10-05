SHELL := /bin/bash

up:
	docker compose --env-file .env up -d --build
	docker compose logs -f --tail=50

down:
	docker compose down -v

seed:
	python scripts/seed_sample.py

backfill-month:
	@echo "Usage: make backfill-month YYYY-MM" && exit 0