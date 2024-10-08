GOCC=go
MKDIR_P=mkdir -p
GIT_SUBM=git submodule

BIN_PATH=./build
BIN="./build/armiarma"

DOCKER_VOLUMES="./app-data/"

.PHONY: build dependencies install clean clean-volumes

build:
	$(GOCC) get
	$(GOCC) build -o $(BIN)

dependencies:
	$(GIT_SUBM) update --init --recursive

install:
	$(GOCC) install
	
clean:
	rm -r $(BIN_PATH)

clean-volumes:
	sudo rm -rf $(DOCKER_VOLUMES)*_db
