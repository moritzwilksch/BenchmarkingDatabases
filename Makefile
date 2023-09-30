install:
	pip install -r requirements.txt

docker/run-postgres: 
	docker run --name postgres --env-file .env -p 5432:5432 -d postgres

docker/run-mongo: 
	docker run --name mongo -p 27017:27017 --env-file .env -d mongo

docker-run-surreal:
	docker run --rm --name surrealdb -p 1943:8000 surrealdb/surrealdb:latest start --log trace --user moritz --pass suedkreuzBhF memory

docker/clean:
	docker rm postgres
	docker rm mongo