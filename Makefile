.PHONY: test


test: redis .env
	python -m unittest;

redis:
	docker-compose up -d redis;

.env:
	cp .env.example .env;