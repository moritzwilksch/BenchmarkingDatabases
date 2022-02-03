install:
	pip install -r requirements.txt

docker/run-postgres: 
	docker run --name postgres --env-file .env -p 5432:5432 -d postgres

docker/run-mongo: 
	docker run --name mongo -p 27017:27017 --env-file .env -d mongo

docker/clean:
	docker rm postgres
	docker rm mongo