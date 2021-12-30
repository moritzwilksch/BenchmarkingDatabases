install:
	pip install -r requirements.txt

docker/run-postgres: 
	docker run --name postgres -e POSTGRES_PASSWORD=postgres -p 5432:5432 -d postgres

docker/run-mongo: 
	docker run --name mongo -p 27017:27017 -d mongo

docker/clean:
	docker rm postgres
	docker rm mongo