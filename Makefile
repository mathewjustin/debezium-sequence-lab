.PHONY: demo test clean logs status

demo:
	./demo.sh

test:
	./scripts/run.sh

clean:
	docker compose --project-name debezium-sequence-lab down --volumes --remove-orphans

logs:
	docker compose --project-name debezium-sequence-lab logs --tail 200

status:
	curl --fail --silent http://localhost:$${CONNECT_PORT:-58083}/connectors/debezium-source/status
	@printf '\n'
	curl --fail --silent http://localhost:$${CONNECT_PORT:-58083}/connectors/debezium-sink/status
	@printf '\n'
