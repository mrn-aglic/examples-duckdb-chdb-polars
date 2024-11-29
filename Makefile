#run-tests:
#	docker compose up app_run_tests

run-csv-redis:
	docker compose up redis app_store_to_redis
