re: build_app build_app_container up
build_app:
	go build -v -o ./build/app ./cmd/main.go
build_app_container:
	docker-compose -f docker-compose.yml -f docker-compose-development.yml build app
up:
	docker-compose -f docker-compose.yml -f docker-compose-development.yml up -d
upr:
	docker-compose -f docker-compose.yml -f docker-compose-development.yml up -d --force-recreate
stop:
	docker-compose -f docker-compose.yml -f docker-compose-development.yml stop
sh:
	docker-compose -f docker-compose.yml -f docker-compose-development.yml exec app sh
test:
	docker-compose -f docker-compose.yml -f docker-compose-development.yml -f docker-compose-test.yml up
