# Run our formatting checks
ruff:
	@ruff check --fix .
	@ruff format .


# Run the Docker container
docker:
	@docker compose up --build